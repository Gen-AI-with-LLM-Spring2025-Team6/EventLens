import torch
import streamlit as st
from openai import OpenAI
from sentence_transformers import SentenceTransformer, util

def get_openai_embedding(text, openai_api_key):
    """
    Use OpenAI's text-embedding-ada-002 model to get an embedding for a text.
    """
    client = OpenAI(api_key=openai_api_key)
    try:
        response = client.embeddings.create(
            input=[text],
            model="text-embedding-ada-002"
        )
        embedding = response.data[0].embedding
        return torch.tensor(embedding)
    except Exception as e:
        st.error(f"Error getting OpenAI embedding: {e}")
        return None

def get_chunk_embeddings(chunks, embed_model, use_openai_embedding=False, openai_api_key=None):
    """
    Generate embeddings for each document chunk.
    If use_openai_embedding is True and an API key is provided, use OpenAI's model.
    Otherwise, use the provided SentenceTransformer model.
    """
    embeddings = []
    if use_openai_embedding and openai_api_key:
        for chunk in chunks:
            emb = get_openai_embedding(chunk, openai_api_key)
            if emb is not None:
                embeddings.append(emb)
        if embeddings:  # Check if list is not empty before stacking
            embeddings = torch.stack(embeddings)
        else:
            return None
    else:
        embeddings = embed_model.encode(chunks, convert_to_tensor=True)
    return embeddings

def vector_search_for_step(step, chunk_embeddings, chunks, embed_model, top_k=3, use_openai_embedding=False, openai_api_key=None):
    """
    For a given reasoning step, compute its embedding (using the chosen method) and then find the top-k
    most similar document chunks using cosine similarity.
    """
    if use_openai_embedding and openai_api_key:
        step_embedding = get_openai_embedding(step, openai_api_key)
        if step_embedding is None:  # Handle the case when embedding fails
            return []
    else:
        step_embedding = embed_model.encode(step, convert_to_tensor=True)
    
    # Make sure we don't request more items than we have
    top_k = min(top_k, len(chunks))
    if top_k == 0:  # No chunks available
        return []
        
    cos_scores = util.cos_sim(step_embedding, chunk_embeddings)[0]
    top_results = torch.topk(cos_scores, k=top_k)
    top_chunks = []
    for score, idx in zip(top_results[0], top_results[1]):
        top_chunks.append((chunks[idx], score.item()))
    return top_chunks