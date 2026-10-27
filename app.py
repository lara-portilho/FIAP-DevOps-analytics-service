import os
import sys
import threading
import json
import uuid
import time
import logging
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from flask import Flask, jsonify
from dotenv import load_dotenv
from opentelemetry import propagate, trace
from opentelemetry.instrumentation.utils import suppress_instrumentation
from opentelemetry.trace import SpanKind, Status, StatusCode


class JsonFormatter(logging.Formatter):
    """ Log em JSON (uma linha por evento) para o Alloy/Loki. Os campos otelTraceID e
    otelSpanID são injetados pela instrumentação de logging do OpenTelemetry
    (OTEL_PYTHON_LOG_CORRELATION=true) e permitem pular do log para o trace. """

    def format(self, record):
        entry = {
            "time": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        trace_id = getattr(record, "otelTraceID", "0")
        if trace_id and trace_id != "0":
            entry["trace_id"] = trace_id
            entry["span_id"] = getattr(record, "otelSpanID", "")
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


# Configura o logging (force=True substitui o handler que o opentelemetry-instrument
# já tenha criado, mantendo a injeção de trace_id/span_id nos registros)
_handler = logging.StreamHandler()
_handler.setFormatter(JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler], force=True)
log = logging.getLogger(__name__)

tracer = trace.get_tracer("analytics-service")

# Carrega .env para desenvolvimento local
load_dotenv()

# --- Configuração ---
AWS_REGION = os.getenv("AWS_REGION")
SQS_QUEUE_URL = os.getenv("AWS_SQS_URL")
DYNAMODB_TABLE_NAME = os.getenv("AWS_DYNAMODB_TABLE")
DYNAMODB_URL = os.getenv("AWS_DYNAMODB_URL")

if not all([AWS_REGION, SQS_QUEUE_URL, DYNAMODB_TABLE_NAME]):
    log.critical("Erro: AWS_REGION, AWS_SQS_URL, e AWS_DYNAMODB_TABLE devem ser definidos.")
    sys.exit(1)

# --- Clientes Boto3 ---
# Criamos a sessão uma vez
try:
    session = boto3.Session(region_name=AWS_REGION)
    sqs_client = session.client("sqs", endpoint_url=SQS_QUEUE_URL)
    dynamodb_client = session.client("dynamodb", endpoint_url=DYNAMODB_URL)
    log.info(f"Clientes Boto3 inicializados na região {AWS_REGION}")
except NoCredentialsError:
    log.critical("Credenciais da AWS não encontradas. Verifique seu ambiente.")
    sys.exit(1)
except Exception as e:
    log.critical(f"Erro ao inicializar o Boto3: {e}")
    sys.exit(1)


# --- SQS Worker ---

def process_message(message):
    """ Processa uma única mensagem SQS e a insere no DynamoDB.

    O evaluation-service envia o contexto do trace (traceparent) nos
    MessageAttributes; o span CONSUMER criado aqui continua esse trace, então o
    APM mostra evaluation -> analytics no mesmo trace/Service Map. As chamadas
    ao DynamoDB e ao SQS (botocore instrumentado) ficam como filhas dele.
    """
    carrier = {
        key: attr["StringValue"]
        for key, attr in message.get("MessageAttributes", {}).items()
        if attr.get("DataType") == "String"
    }
    parent_ctx = propagate.extract(carrier)
    queue_name = SQS_QUEUE_URL.rstrip("/").split("/")[-1]

    with tracer.start_as_current_span(
        f"process {queue_name}",
        context=parent_ctx,
        kind=SpanKind.CONSUMER,
        attributes={
            "messaging.system": "aws_sqs",
            "messaging.operation.type": "process",
            "messaging.destination.name": queue_name,
            "messaging.message.id": message["MessageId"],
        },
    ) as span:
        _process_message(message, span)


def _process_message(message, span):
    try:
        log.info(f"Processando mensagem ID: {message['MessageId']}")
        body = json.loads(message['Body'])

        # Gera um ID único para o item no DynamoDB
        event_id = str(uuid.uuid4())

        # Constrói o item no formato do DynamoDB
        item = {
            'event_id': {'S': event_id},
            'user_id': {'S': body['user_id']},
            'flag_name': {'S': body['flag_name']},
            'result': {'BOOL': body['result']},
            'timestamp': {'S': body['timestamp']}
        }

        # Insere no DynamoDB
        dynamodb_client.put_item(
            TableName=DYNAMODB_TABLE_NAME,
            Item=item
        )

        span.set_attribute("flag.name", body['flag_name'])
        log.info(f"Evento {event_id} (Flag: {body['flag_name']}) salvo no DynamoDB.")

        # Se tudo deu certo, deleta a mensagem da fila
        sqs_client.delete_message(
            QueueUrl=SQS_QUEUE_URL,
            ReceiptHandle=message['ReceiptHandle']
        )

    except json.JSONDecodeError as e:
        span.record_exception(e)
        span.set_status(Status(StatusCode.ERROR, "JSON inválido"))
        log.error(f"Erro ao decodificar JSON da mensagem ID: {message['MessageId']}")
        # Não deleta a mensagem, pode ser uma "poison pill"
    except ClientError as e:
        span.record_exception(e)
        span.set_status(Status(StatusCode.ERROR, "erro do Boto3"))
        log.error(f"Erro do Boto3 (DynamoDB ou SQS) ao processar {message['MessageId']}: {e}")
        # Não deleta a mensagem, tenta novamente
    except Exception as e:
        span.record_exception(e)
        span.set_status(Status(StatusCode.ERROR, "erro inesperado"))
        log.error(f"Erro inesperado ao processar {message['MessageId']}: {e}")
        # Não deleta a mensagem, tenta novamente


def sqs_worker_loop():
    """ Loop principal do worker que ouve a fila SQS """
    log.info("Iniciando o worker SQS...")
    while True:
        try:
            # Long-polling: espera até 20s por mensagens. O span do ReceiveMessage é
            # suprimido para não gerar um trace vazio a cada 20s no APM.
            with suppress_instrumentation():
                response = sqs_client.receive_message(
                    QueueUrl=SQS_QUEUE_URL,
                    MaxNumberOfMessages=10,  # Processa em lotes de até 10
                    WaitTimeSeconds=20,
                    MessageAttributeNames=["All"]  # traz o traceparent enviado pelo evaluation-service
                )

            messages = response.get('Messages', [])
            if not messages:
                # Nenhuma mensagem, continua o loop
                continue

            log.info(f"Recebidas {len(messages)} mensagens.")

            for message in messages:
                process_message(message)

        except ClientError as e:
            log.error(f"Erro do Boto3 no loop principal do SQS: {e}")
            time.sleep(10)  # Pausa antes de tentar novamente
        except Exception as e:
            log.error(f"Erro inesperado no loop principal do SQS: {e}")
            time.sleep(10)

# --- Servidor Flask (Apenas para Health Check) ---


app = Flask(__name__)


@app.route('/health')
def health():
    # Uma verificação de saúde real poderia checar a conexão com o DynamoDB/SQS
    return jsonify({"status": "ok"})

# --- Inicialização ---


def start_worker():
    """ Inicia o worker SQS em uma thread separada """
    worker_thread = threading.Thread(target=sqs_worker_loop, daemon=True)
    worker_thread.start()


# Inicia o worker SQS em uma thread de background
# Isso garante que ele inicie tanto com 'flask run' quanto com 'gunicorn'
start_worker()

if __name__ == '__main__':
    port = int(os.getenv("PORT", 8005))
    app.run(host='0.0.0.0', port=port, debug=False)
