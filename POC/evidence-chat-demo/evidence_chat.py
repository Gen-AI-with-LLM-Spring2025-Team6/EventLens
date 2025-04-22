import streamlit as st
import openai
from openai import OpenAI
import pymupdf
import io
import re
import nltk
import torch
from sentence_transformers import SentenceTransformer, util
import anthropic
import requests
import json


# ---------------------------
# Document Processing Functions
# ---------------------------
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

# ---------------------------
# Embedding Functions
# ---------------------------
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

def vector_search(query, chunk_embeddings, chunks, embed_model, top_k=5, use_openai_embedding=False, openai_api_key=None):
    """
    Compute query embedding and find the top-k most similar document chunks using cosine similarity.
    """
    if use_openai_embedding and openai_api_key:
        query_embedding = get_openai_embedding(query, openai_api_key)
        if query_embedding is None:  # Handle the case when embedding fails
            return []
    else:
        query_embedding = embed_model.encode(query, convert_to_tensor=True)
    
    # Make sure we don't request more items than we have
    top_k = min(top_k, len(chunks))
    if top_k == 0:  # No chunks available
        return []
        
    cos_scores = util.cos_sim(query_embedding, chunk_embeddings)[0]
    top_results = torch.topk(cos_scores, k=top_k)
    top_chunks = []
    for score, idx in zip(top_results[0], top_results[1]):
        top_chunks.append((chunks[idx], score.item()))
    return top_chunks

# ---------------------------
# LLM Interaction Functions
# ---------------------------
def generate_openai_response(system_prompt, user_prompt, model_name, api_key, max_tokens=500, temperature=0.2):
    """
    Generate response using OpenAI models.
    """
    client = OpenAI(api_key=api_key)
    try:
        response = client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=temperature,
            max_tokens=max_tokens
        )
        return response.choices[0].message.content
    except Exception as e:
        st.error(f"Error with OpenAI API: {e}")
        return None

def generate_claude_response(system_prompt, user_prompt, model_name, api_key, max_tokens=500, temperature=0.2):
    """
    Generate response using Anthropic's Claude models.
    """
    client = anthropic.Anthropic(api_key=api_key)
    try:
        response = client.messages.create(
            model=model_name,
            system=system_prompt,
            messages=[
                {"role": "user", "content": user_prompt}
            ],
            max_tokens=max_tokens,
            temperature=temperature
        )
        return response.content[0].text
    except Exception as e:
        st.error(f"Error with Claude API: {e}")
        return None

def generate_llama_response(system_prompt, user_prompt, model_name, api_key, api_url, max_tokens=500, temperature=0.2):
    """
    Generate response using Llama models via API.
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    data = {
        "model": model_name,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "max_tokens": max_tokens,
        "temperature": temperature
    }
    
    try:
        response = requests.post(api_url, headers=headers, data=json.dumps(data))
        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"]
        else:
            st.error(f"Error with Llama API. Status code: {response.status_code}. Message: {response.text}")
            return None
    except Exception as e:
        st.error(f"Error with Llama API: {e}")
        return None

def generate_llm_response(llm_type, system_prompt, user_prompt, api_key, api_config, max_tokens=500, temperature=0.2):
    """
    Route to the appropriate LLM based on the selected type.
    """
    if llm_type == "OpenAI":
        return generate_openai_response(
            system_prompt, user_prompt, 
            api_config["model_name"], api_key, 
            max_tokens, temperature
        )
    elif llm_type == "Claude":
        return generate_claude_response(
            system_prompt, user_prompt, 
            api_config["model_name"], api_key, 
            max_tokens, temperature
        )
    elif llm_type == "Llama":
        return generate_llama_response(
            system_prompt, user_prompt, 
            api_config["model_name"], api_key, 
            api_config["api_url"], max_tokens, temperature
        )
    else:
        st.error(f"Unknown LLM type: {llm_type}")
        return None

# ---------------------------
# One-Shot Answer Generation
# ---------------------------
def generate_one_shot_answer(question, relevant_chunks, llm_type, api_key, api_config):
    """
    Generate a direct answer based on the question and relevant chunks.
    """
    # Format the context by combining chunks
    context = "\n\n".join([chunk for chunk, _ in relevant_chunks])
    
    system_prompt = (
        "You are an expert question-answering assistant. Given a question and relevant context from a document, "
        "provide a direct, comprehensive answer based solely on the information in the context. "
        "Your answer should be factual and directly tied to the information in the context."
    )
    
    user_prompt = f"Question: {question}\n\nContext from document:\n{context}\n\nAnswer:"
    
    return generate_llm_response(
        llm_type, system_prompt, user_prompt, 
        api_key, api_config, max_tokens=400, temperature=0.2
    ) or "Could not generate an answer."

# ---------------------------
# Evidence Extraction Function
# ---------------------------
def extract_supporting_evidence(question, answer, relevant_chunks, llm_type, api_key, api_config):
    """
    Extract specific sentences from the chunks that serve as evidence for the answer.
    """
    # Format the context by combining chunks
    context = "\n\n".join([chunk for chunk, _ in relevant_chunks])
    
    system_prompt = (
        "You are an evidence extraction assistant. Given a question, an answer, and context from a document, "
        "extract up to 3 specific sentences from the context that best support the answer. "
        "Only extract sentences that actually appear in the context. Do not modify or create new sentences."
    )
    
    user_prompt = (
        f"Question: {question}\n\n"
        f"Answer: {answer}\n\n"
        f"Context: {context}\n\n"
        "Extract up to 3 sentences from the context that serve as evidence for the answer:"
    )
    
    evidence = generate_llm_response(
        llm_type, system_prompt, user_prompt, 
        api_key, api_config, max_tokens=300, temperature=0.0
    )
    
    return evidence or "Could not extract supporting evidence."

# ---------------------------
# Correctness Evaluation Function
# ---------------------------
def evaluate_correctness(question, answer, document_text, llm_type, api_key, api_config):
    """
    Uses the selected LLM to evaluate the correctness of the answer based on the question and document.
    Returns a score from 0 to 1 indicating correctness.
    """
    system_prompt = (
        "You are an expert evaluator. Your task is to assess the correctness of an answer to a question "
        "based on information from a document. "
        "Provide a correctness score between 0 and 1, where 0 means completely incorrect or unsupported, "
        "and 1 means completely correct and fully supported by the document. "
        "You must provide your reasoning before giving the final score."
    )
    
    # Truncate document if too long to fit in context
    doc_preview = document_text[:3000] + ("..." if len(document_text) > 3000 else "")
    
    user_prompt = (
        f"Question: {question}\n\n"
        f"Answer to evaluate: {answer}\n\n"
        f"Document excerpt: {doc_preview}\n\n"
        "First provide your reasoning for the evaluation. Then on a new line, "
        "provide just the correctness score as a decimal between 0 and 1. "
        "For example: 'Score: 0.85'"
    )
    
    output = generate_llm_response(
        llm_type, system_prompt, user_prompt, 
        api_key, api_config, max_tokens=350, temperature=0.2
    )
    
    if output:
        # Extract the score (assuming it's in format "Score: X.XX")
        score_match = re.search(r'Score:\s*(\d*\.\d+|\d+)', output)
        if score_match:
            score = float(score_match.group(1))
            # Ensure score is within [0, 1]
            score = max(0, min(1, score))
            reasoning = output.split("Score:")[0].strip()
            return score, reasoning
        else:
            # Fallback if regex doesn't match - try to extract any float from the text
            float_matches = re.findall(r'(\d*\.\d+|\d+)', output)
            for match in float_matches:
                try:
                    score = float(match)
                    if 0 <= score <= 1:
                        return score, output.replace(match, "").strip()
                except:
                    pass
            
            # If no valid score is found
            return 0.5, "Could not extract a clear score. Default score assigned."
    else:
        return 0.5, "Error during evaluation. Default score assigned."

# ---------------------------
# Main Streamlit App
# ---------------------------
def main():
    st.title("One-Shot EvidenceChat")
    st.write("Upload a document and ask questions. The app will find relevant information and generate an answer with supporting evidence.")
    
    # Initialize session state for storing document and embeddings
    if 'document_text' not in st.session_state:
        st.session_state.document_text = None
    if 'chunks' not in st.session_state:
        st.session_state.chunks = None
    if 'chunk_embeddings' not in st.session_state:
        st.session_state.chunk_embeddings = None
    if 'embed_model' not in st.session_state:
        st.session_state.embed_model = None

    # Sidebar: LLM selection and API configuration
    llm_type = st.sidebar.selectbox(
        "Select Language Model Provider",
        ["OpenAI", "Claude", "Llama"]
    )
    
    # API key input based on selected LLM
    if llm_type == "OpenAI":
        api_key = st.sidebar.text_input("Enter OpenAI API Key", type="password")
        api_config = {
            "model_name": st.sidebar.selectbox(
                "Select OpenAI Model",
                ["gpt-4o-mini", "gpt-4o", "gpt-4-turbo", "gpt-3.5-turbo"]
            )
        }
        use_openai_embed = st.sidebar.checkbox("Use OpenAI for text encoding (text-embedding-ada-002)", value=True)
        embedding_api_key = api_key
    elif llm_type == "Claude":
        api_key = st.sidebar.text_input("Enter Anthropic API Key", type="password")
        api_config = {
            "model_name": st.sidebar.selectbox(
                "Select Claude Model",
                ["claude-3-5-sonnet-20240620", "claude-3-opus-20240229", "claude-3-haiku-20240307"]
            )
        }
        use_openai_embed = False
        embedding_api_key = st.sidebar.text_input("Enter OpenAI API Key for embeddings (optional)", type="password") if st.sidebar.checkbox("Use OpenAI for embeddings") else None
    elif llm_type == "Llama":
        api_key = st.sidebar.text_input("Enter Llama API Key", type="password")
        api_config = {
            "model_name": st.sidebar.selectbox(
                "Select Llama Model",
                ["llama-3-70b", "llama-3-8b", "llama-2-70b"]
            ),
            "api_url": st.sidebar.text_input("Llama API Endpoint URL", value="https://api.llama.your-provider.com/v1/chat/completions")
        }
        use_openai_embed = False
        embedding_api_key = st.sidebar.text_input("Enter OpenAI API Key for embeddings (optional)", type="password") if st.sidebar.checkbox("Use OpenAI for embeddings") else None
    
    # Embedding model parameters
    if not use_openai_embed and 'embed_model' not in st.session_state:
        with st.spinner("Loading embedding model..."):
            st.session_state.embed_model = SentenceTransformer('all-MiniLM-L6-v2')
    
    # Document Upload
    uploaded_file = st.file_uploader("Upload a PDF or TXT document", type=["pdf", "txt"])
    
    if uploaded_file and (st.session_state.document_text is None or uploaded_file.name != st.session_state.get('last_uploaded_filename', '')):
        with st.spinner("Processing document..."):
            st.session_state.document_text = load_document(uploaded_file)
            st.session_state.last_uploaded_filename = uploaded_file.name
            if st.session_state.document_text:
                # Split document into chunks and get embeddings
                st.session_state.chunks = chunk_text(st.session_state.document_text, chunk_size=200)
                
                if use_openai_embed and embedding_api_key:
                    st.session_state.chunk_embeddings = get_chunk_embeddings(
                        st.session_state.chunks, None, 
                        use_openai_embedding=True, 
                        openai_api_key=embedding_api_key
                    )
                else:
                    st.session_state.chunk_embeddings = get_chunk_embeddings(
                        st.session_state.chunks, st.session_state.embed_model, 
                        use_openai_embedding=False
                    )
                
                st.success(f"Document processed: {len(st.session_state.chunks)} chunks created")
            else:
                st.error("Failed to load document")
    
    # Question input
    question = st.text_input("Ask a question about the document:")
    
    # Search parameters
    with st.expander("Search Parameters"):
        top_k = st.slider("Number of relevant chunks to retrieve:", 1, 10, 5)
        temperature = st.slider("LLM Temperature:", 0.0, 1.0, 0.2)
    
    # Process question
    if question and st.session_state.document_text and st.session_state.chunks and st.session_state.chunk_embeddings is not None:
        if st.button("Generate Answer"):
            with st.spinner("Searching document and generating answer..."):
                # Find relevant chunks
                relevant_chunks = vector_search(
                    question, st.session_state.chunk_embeddings, st.session_state.chunks,
                    st.session_state.embed_model, top_k=top_k,
                    use_openai_embedding=use_openai_embed, openai_api_key=embedding_api_key
                )
                
                if not relevant_chunks:
                    st.error("No relevant information found in the document.")
                else:
                    # Display retrieved chunks in expander
                    with st.expander("Retrieved Document Chunks", expanded=False):
                        for i, (chunk, score) in enumerate(relevant_chunks):
                            st.write(f"**Chunk {i+1}** (Relevance: {score:.4f})")
                            st.write(chunk)
                            st.write("---")
                    
                    # Generate answer
                    answer = generate_one_shot_answer(
                        question, relevant_chunks, llm_type, api_key, api_config
                    )
                    
                    # Extract supporting evidence
                    evidence = extract_supporting_evidence(
                        question, answer, relevant_chunks, llm_type, api_key, api_config
                    )
                    
                    # Evaluate answer correctness
                    correctness_score, reasoning = evaluate_correctness(
                        question, answer, st.session_state.document_text, 
                        llm_type, api_key, api_config
                    )
                    
                    # Display results
                    st.markdown("### Answer")
                    st.write(answer)
                    
                    st.markdown("### Supporting Evidence")
                    st.write(evidence)
                    
                    st.markdown("### Correctness Evaluation")
                    st.progress(correctness_score)
                    st.write(f"Confidence Score: {correctness_score:.2f}")
                    
                    with st.expander("Evaluation Reasoning"):
                        st.write(reasoning)
    elif st.session_state.document_text is None:
        st.info("Please upload a document to get started.")
    elif not question:
        st.info("Enter a question to search the document.")

# Run the app
if __name__ == "__main__":
    main()