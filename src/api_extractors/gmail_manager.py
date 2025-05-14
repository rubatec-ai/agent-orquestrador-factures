import logging
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from typing import Dict, Optional, Tuple
import pandas as pd
import base64
import os
import pickle
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from src.config import ConfigurationManager
from src.api_extractors.base_extractor import BaseExtractor
from src.utils.constants import MAX_RESULTS_GMAIL
from email.utils import parsedate_to_datetime, parseaddr
from email.mime.text import MIMEText
import pytz

from src.utils.utils import compute_hash


class GmailManager(BaseExtractor):
    """
    Extrae y procesa datos de Gmail usando credenciales OAuth 2.0.

    Esta clase:
      - Carga las credenciales vía OAuth (o desde un token almacenado localmente).
      - Crea un cliente de la API de Gmail.
      - Obtiene los hilos (threads) filtrados por start_date y label (según parámetros en la config).
      - Para cada hilo, recorre todos los mensajes (respuesta, mensaje inicial, etc).
      - Para cada mensaje extrae información relevante (fecha, asunto, remitente, cuerpo).
      - Además, para los mensajes que tengan PDFs adjuntos, crea una fila por cada PDF.
      - Genera dos DataFrames: uno "master" con todos los mensajes del Gmail y otro con solo los adjuntos PDF.
    """

    def __init__(self, config: ConfigurationManager) -> None:
        self._config = config
        self._client_secret_file = config.google_gmail_client_secret_file  # e.g. "credentials/client_secret.json"
        self._token_file = config.google_gmail_token_file  # e.g. "credentials/token.pickle"
        self._scopes = config.google_gmail_scopes  # e.g. ["https://www.googleapis.com/auth/gmail.readonly"]

        creds = self._get_oauth_credentials()
        self._service = build('gmail', 'v1', credentials=creds)

        # Carpeta para guardar los adjuntos PDF (temporal)
        self._save_pdf_folder = Path(config.gmail_save_pdf_attachments_folder)

        # Parámetros de filtrado
        self._start_date = config.gmail_start_date  # e.g., "2023/06/01"
        self._label = config.gmail_label  # e.g., "INBOX"

        self._logger = logging.getLogger("GmailExtractor")
        self._logger.info('Starting Gmail Extractor..')
        super().__init__(config)

    def _get_oauth_credentials(self):
        """
        Obtiene las credenciales OAuth.
        - Si existe un token (self._token_file), lo carga.
        - Si no, ejecuta el flujo OAuth y guarda el token.
        """
        creds = None

        # Asegurarse de que exista el directorio para el token:
        token_dir = self._config.data_directory
        if token_dir and not os.path.exists(token_dir):
            os.makedirs(token_dir, exist_ok=True)

        # Cargar token si existe
        if os.path.exists(self._token_file):
            with open(self._token_file, 'rb') as token:
                creds = pickle.load(token)

        # Si las credenciales no son válidas, se inicia el flujo OAuth
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self._client_secret_file, self._scopes
                )
                creds = flow.run_local_server(port=0)
            # Guardar credenciales para la próxima ejecución
            with open(self._token_file, 'wb') as token:
                pickle.dump(creds, token)
        return creds

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Obtiene los hilos (threads) filtrados por start_date y label, y para cada hilo:
          - Recorre cada mensaje para extraer información (fecha, asunto, remitente, cuerpo).
          - Recorre los adjuntos para extraer PDF (una fila por PDF).
        Genera dos DataFrames:
          - "messages": cada fila representa un mensaje dentro del hilo.
          - "invoices": cada fila representa un adjunto PDF (sin incluir el cuerpo del mensaje).
        """
        try:
            query = self._build_query()
            # Usamos threads.list para obtener los hilos completos
            list_response = self._service.users().threads().list(
                userId='me', q=query, maxResults=MAX_RESULTS_GMAIL
            ).execute()
            threads = list_response.get('threads', [])
            messages_data = []
            attachments_data = []

            for thread in threads:
                thread_id = thread["id"]
                thread_details = self._service.users().threads().get(
                    userId='me', id=thread_id, format="full"
                ).execute()

                for msg in thread_details.get("messages", []):
                    msg_id = msg.get("id", "")
                    headers = msg.get("payload", {}).get("headers", [])
                    subject = ""
                    date_received = ""
                    sender = ""
                    for h in headers:
                        header_name = h.get("name", "").lower()
                        if header_name == "subject":
                            subject = h.get("value", "")
                        elif header_name == "date":
                            raw_date_str = h.get("value", "")
                            date_received = self._parse_date(raw_date_str)
                        elif header_name == "from":
                            sender = h.get("value", "")

                    # Extraer cuerpo usando una función recursiva
                    body = self._extract_body(msg)

                    # Agregar información al master de mensajes
                    messages_data.append({
                        "message_id": msg_id,
                        "thread_id": thread_id,
                        "subject": subject,
                        "date_received": date_received,
                        "sender": sender,
                        "body": body
                    })

                    # Procesar adjuntos: se crea una fila por cada PDF adjunto.
                    parts = msg.get("payload", {}).get("parts", [])
                    if parts:
                        for part in parts:
                            filename = part.get("filename", "")
                            if filename and filename.lower().endswith(".pdf"):
                                attachment_id = part.get("body", {}).get("attachmentId", "")
                                local_path, hash_invoice = self._download_pdf_attachment(msg_id, attachment_id,filename)
                                attachments_data.append({
                                    "message_id": msg_id,
                                    "thread_id": thread_id,
                                    "subject": subject,
                                    "date_received": date_received,
                                    "sender": sender,
                                    "attachment_id": attachment_id,
                                    "filename": filename,
                                    "pdf_local_path": local_path,
                                    "hash": hash_invoice
                                })
                    # En caso de que no exista "parts", se puede tener el cuerpo directo y sin adjuntos.

            df_messages = pd.DataFrame(messages_data)
            df_messages.sort_values(by=['date_received'], ascending=True, inplace=True)
            df_attachments = pd.DataFrame(attachments_data)
            # Retornamos ambos dataframes; luego se pueden limpiar o asignar en clean_input_data según convenga.
            return {"messages": df_messages, "invoices": df_attachments}
        except Exception as e:
            raise Exception(f"Failed to fetch Gmail data: {str(e)}")

    def clean_input_data(self):
        """
        Asigna los DataFrames extraídos a la variable _clean_inputs.
        Se asigna el master de mensajes y el de adjuntos PDF.
        """
        raw_messages = self._raw_inputs.get("messages")
        raw_attachments = self._raw_inputs.get("invoices")
        if raw_messages is not None and not raw_messages.empty:
            self._clean_inputs["messages"] = raw_messages
        if raw_attachments is not None and not raw_attachments.empty:
            self._clean_inputs["invoices"] = raw_attachments

    def _build_query(self) -> str:
        """Construye la query para la API de Gmail usando start_date y label."""
        query_parts = []
        if self._start_date:
            query_parts.append(f"after:{self._start_date}")
        if self._label:
            query_parts.append(f"label:{self._label}")
        return " ".join(query_parts).strip()

    def _extract_body(self, msg: dict) -> str:
        """
        Extrae el cuerpo en texto plano de un mensaje de forma recursiva.
        Se recorre la estructura de parts para encontrar el primer fragmento en 'text/plain'.
        """

        def extract_text(part: dict) -> str:
            if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
                return self._decode_base64(part.get("body", {}).get("data", ""))
            if "parts" in part:
                for subpart in part.get("parts", []):
                    text = extract_text(subpart)
                    if text:
                        return text
            return ""

        payload = msg.get("payload", {})
        return extract_text(payload)

    def _download_pdf_attachment(self, msg_id: str, attachment_id: str, filename: str) -> Optional[Tuple[str, str]]:
        """
        Descarga el adjunto PDF usando la API de Gmail y lo guarda en la carpeta configurada.
        Retorna la ruta local del archivo y su hash MD5.
        """
        if not self._save_pdf_folder:
            return None

        try:
            # Obtener el adjunto desde Gmail
            attachment = self._service.users().messages().attachments().get(
                userId='me', messageId=msg_id, id=attachment_id
            ).execute()
            data = attachment.get('data', '')
            pdf_content = base64.urlsafe_b64decode(data.encode('utf-8'))
            pdf_hash = compute_hash(pdf_content, 'md5')

            # Asegurar que la carpeta existe
            save_folder = Path(self._save_pdf_folder)
            save_folder.mkdir(parents=True, exist_ok=True)

            # Guardar el PDF y devolver la ruta
            local_path = save_folder / filename
            with local_path.open('wb') as f:
                f.write(pdf_content)

            return str(local_path), pdf_hash

        except Exception as e:
            self._logger.warning(
                f"Error descargando adjunto {attachment_id} del mensaje {msg_id}: {e}"
            )
            return None

    def _decode_base64(self, data: str) -> str:
        """Decodifica una cadena base64 url-safe a UTF-8."""
        if not data:
            return ""
        try:
            return base64.urlsafe_b64decode(data.encode("UTF-8")).decode("UTF-8")
        except Exception:
            return ""

    @staticmethod
    def _parse_date(date_str: str) -> str:
        """
        Convierte una cadena de fecha/hora de la cabecera 'Date' de un email
        a un string con el formato 'YYYY-MM-DD HH:MM:SS' en horario de Madrid.
        Si no se puede parsear, retorna la cadena original.
        """
        try:
            # 1) Parsear la fecha RFC2822/RFC5322 a objeto datetime (tz-aware si incluye zona horaria).
            dt = parsedate_to_datetime(date_str)
            # 2) Convertir la fecha a la zona horaria de Madrid.
            spain_tz = pytz.timezone("Europe/Madrid")
            dt_spain = dt.astimezone(spain_tz)
            # 3) Devolver en el formato deseado.
            return dt_spain.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            # Si no se puede parsear, se devuelve el string original (o podrías retornar None).
            return date_str

    def send_email(self, recipient: str, subject: str, body_text: str, thread_id: str = None,
                   attachment_path: str = None):
        # Crear un mensaje multipart
        message = MIMEMultipart()
        _, recipient_email = parseaddr(recipient)
        message['to'] = recipient_email
        message['subject'] = subject
        if thread_id:
            message['In-Reply-To'] = thread_id
            message['References'] = thread_id

        # Adjuntar el cuerpo del mensaje como parte de texto
        text_part = MIMEText(body_text, "plain")
        message.attach(text_part)

        # Si se provee una ruta para el adjunto y el archivo existe, agregarlo
        if attachment_path and os.path.exists(attachment_path):
            filename = os.path.basename(attachment_path)
            with open(attachment_path, "rb") as f:
                attachment_data = f.read()
            attachment_part = MIMEBase("application", "octet-stream")
            attachment_part.set_payload(attachment_data)
            encoders.encode_base64(attachment_part)
            attachment_part.add_header("Content-Disposition", f"attachment; filename={filename}")
            message.attach(attachment_part)

        # Codificar el mensaje completo en base64
        raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
        message_body = {'raw': raw_message}
        if thread_id:
            message_body['threadId'] = thread_id

        # Enviar el mensaje usando la API de Gmail
        sent_message = self._service.users().messages().send(userId='me', body=message_body).execute()
        return sent_message
