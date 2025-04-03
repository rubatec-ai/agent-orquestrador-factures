import logging
from typing import Dict, List, Optional
import os
import pandas as pd
from logging import Logger
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from src.config import ConfigurationManager
from src.api_extractors.base_extractor import BaseExtractor
from src.utils.constants import PAGE_SIZE_DRIVE


class DriveManager(BaseExtractor):
    """
    Administra operaciones en Google Drive usando credenciales de service account.

    Esta clase:
      - Carga las credenciales desde un archivo JSON.
      - Crea un cliente de la API de Drive.
      - Recorre recursivamente un folder base y sus subfolders para listar todos los archivos PDF,
        incluyendo el path relativo.
      - Permite crear directorios (folders) en Drive.
      - Permite subir archivos PDF a un directorio (usando el path relativo o el folder_id).
      - Es fácilmente extendible para agregar funcionalidades adicionales, por ejemplo, para generar Google Sheets.
    """

    def __init__(self, config: ConfigurationManager) -> None:
        self._credentials_path = config._google_credentials_json
        self._scopes = config.google_drive_scopes
        credentials = service_account.Credentials.from_service_account_file(
            self._credentials_path, scopes=self._scopes
        )
        self._service = build('drive', 'v3', credentials=credentials)
        # Folder ID base donde se realizarán las operaciones
        self._folder_id = config.google_drive_folder_id

        self._logger  = logging.getLogger("DriveManager")
        self._logger .info('Starting Drive Manager..')
        super().__init__(config)

    def _list_files_recursive(self, folder_id: str, current_path: str = "") -> List[dict]:
        """
        Recorre recursivamente el folder indicado y sus subfolders para listar todos los archivos PDF.
        Para cada archivo PDF se añade una clave 'relative_path' que indica el path relativo desde el folder base.
        """
        files_list = []
        query = f"'{folder_id}' in parents"
        response = self._service.files().list(
            q=query,
            pageSize=PAGE_SIZE_DRIVE,
            fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime, size, webViewLink, md5Checksum)"
        ).execute()
        items = response.get("files", [])
        for item in items:
            mime_type = item.get("mimeType", "")
            if mime_type == "application/vnd.google-apps.folder":
                new_path = f"{current_path}/{item.get('name')}" if current_path else item.get("name")
                files_list.extend(self._list_files_recursive(item.get("id"), new_path))
            else:
                if mime_type == "application/pdf":
                    item["relative_path"] = current_path
                    files_list.append(item)
        return files_list

    def get_input_data(self) -> Dict[str, pd.DataFrame]:
        """
        Busca de forma recursiva dentro del folder base (y sus subfolders) todos los archivos PDF.
        Genera un DataFrame con la siguiente información:
          - file_id, filename, created_time, modified_time, file_size, web_view_link, relative_path.
        """
        try:
            self._logger.debug(f"Recorriendo recursivamente el folder base: {self._folder_id}")
            pdf_files = self._list_files_recursive(self._folder_id, current_path="")
            self._logger.info(f"Encontrados {len(pdf_files)} archivos PDF en el folder y subfolders.")
            df = pd.DataFrame(pdf_files)
            return {"files": df}
        except Exception as e:
            raise Exception(f"Failed to fetch Drive data: {str(e)}")

    def clean_input_data(self):
        """
        Procesa y limpia los datos crudos extraídos de Drive.
        Renombra las columnas para consistencia.
        """
        df = self._raw_inputs.get("files")
        if df is not None and not df.empty:
            df = df.rename(columns={
                "id": "file_id",
                "name": "filename",
                "createdTime": "created_time",
                "modifiedTime": "modified_time",
                "size": "file_size",
                "webViewLink": "web_view_link"
            })

            df['created_time'] = (
                pd.to_datetime(df['created_time'],
                               format='%Y-%m-%dT%H:%M:%S.%fZ',
                               utc=True, errors='coerce')
                .dt.tz_convert('Europe/Madrid')
                .dt.strftime('%Y-%m-%d %H:%M:%S')
            )

            df['modified_time'] = (
                pd.to_datetime(df['modified_time'],
                               format='%Y-%m-%dT%H:%M:%S.%fZ',
                               utc=True, errors='coerce')
                .dt.tz_convert('Europe/Madrid')
                .dt.strftime('%Y-%m-%d %H:%M:%S')
            )

            self._clean_inputs["files"] = df

    def create_directory(self, name: str, parent_folder_id: Optional[str] = None) -> dict:
        """
        Crea un directorio (folder) en Google Drive.
        Si no se especifica parent_folder_id, se utiliza el folder base (self._folder_id).
        Retorna la metadata del folder creado.
        """
        parent_folder_id = parent_folder_id or self._folder_id
        folder_metadata = {
            'name': name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_folder_id]
        }
        folder = self._service.files().create(
            body=folder_metadata,
            fields='id, name, parents'
        ).execute()
        self._logger.info(f"Directorio creado: {folder.get('name')} (ID: {folder.get('id')})")
        return folder

    def get_or_create_subfolder(self, relative_path: str) -> str:
        """
        Dado un relative_path (por ejemplo, "subfolder1/subfolder2"),
        verifica si los directorios existen dentro del folder base.
        Si no, los crea.
        Retorna el folder_id del directorio final.
        """
        parent_id = self._folder_id
        folders = relative_path.strip("/").split("/")
        for folder_name in folders:
            query = f"'{parent_id}' in parents and name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder'"
            response = self._service.files().list(
                q=query,
                fields="files(id, name)",
                pageSize=1
            ).execute()
            items = response.get("files", [])
            if items:
                parent_id = items[0].get("id")
            else:
                folder = self.create_directory(folder_name, parent_folder_id=parent_id)
                parent_id = folder.get("id")
        return parent_id

    def upload_pdf(self, file_path: str, target_relative_path: Optional[str] = None) -> dict:
        """
        Sube un archivo PDF a Google Drive.
        Si se especifica target_relative_path (por ejemplo, "subfolder1/subfolder2"),
        se sube el archivo a ese directorio; de lo contrario, se sube al folder base.
        Retorna la metadata del archivo subido.
        """
        if target_relative_path:
            target_folder_id = self.get_or_create_subfolder(target_relative_path)
        else:
            target_folder_id = self._folder_id

        filename = os.path.basename(file_path)
        file_metadata = {
            'name': filename,
            'parents': [target_folder_id]
        }
        media = MediaFileUpload(file_path, mimetype='application/pdf')
        file = self._service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name, parents, webViewLink'
        ).execute()
        self._logger.info(f"Archivo subido: {file.get('name')} (ID: {file.get('id')}) en carpeta {target_folder_id}")

        return file
