import io
import re
import nltk
import streamlit as st
import pymupdf



def load_document(uploaded_file):
    """
    Load document text from an uploaded PDF or TXT file.
    """
    if uploaded_file.type == "application/pdf":
        pdf_data = uploaded_file.read()
        memory_buffer = io.BytesIO(pdf_data)
        doc = pymupdf.open(stream=memory_buffer, filetype="pdf")
        text = ""
        for page in doc:
            text += page.get_text()
        return text
    elif uploaded_file.type == "text/plain":
        return uploaded_file.read().decode("utf-8")
    else:
        return None

def chunk_text(text, chunk_size=200):
    """
    Split the text into chunks of approximately `chunk_size` words.
    """
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunk = " ".join(words[i:i+chunk_size])
        chunks.append(chunk)
    return chunks