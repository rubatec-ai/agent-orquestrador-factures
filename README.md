# Agent Extractor

## Short Description
Agent Extractor is a tool that reads invoice PDFs from a designated Google Drive directory, processes them using OCR (e.g., Google Document AI or PDF.co), and then leverages OpenAI to generate two structured data tables: one for general invoice information and another for detailed line items.

## Overview
The project automates an ETL pipeline where each PDF is processed via an API call. The output is a set of dataframes that hold structured invoice data for further analysis.

## Key Features
- **Google Drive Integration:** Automatically retrieves PDF files from a specified directory.
- **OCR Processing:** Utilizes OCR services to extract text from invoices.
- **Data Structuring:** Uses OpenAI's API to generate:
  - A table with general invoice data.
  - A table detailing invoice line items.
- **Scalable ETL:** Processes each PDF individually with a simple API call architecture.

## Architecture & Workflow
1. **File Retrieval:**  
   Connect to Google Drive and list PDFs in the designated folder.
2. **OCR Extraction:**  
   Call an OCR service (Google Document AI, PDF.co, etc.) for text extraction.
3. **Data Structuring:**  
   Use OpenAI to parse and structure the extracted text into two solution tables.
4. **Output:**  
   Final data is presented as pandas dataframes (or your chosen format) for easy analysis.

## Installation & Setup
1. **Clone the Repository:**  
   ```bash
   git clone https://github.com/yourusername/agent-extractor.git
