import streamlit as st
from sentence_transformers import SentenceTransformer

# Import functions from our modules
from utils import load_document, chunk_text
from embeddings import get_chunk_embeddings, vector_search_for_step
from llm_functions import (
    generate_reasoning_steps,
    answer_step_with_evidence,
    generate_final_answer,
    reflection_agent,
    evaluate_correctness
)

def main():
    st.title("EvidenceChat Demo with Multiple LLMs")
    st.write("This demo implements an enhanced version of the EvidenceChat pipeline with support for multiple language models. "
             "It breaks down the reasoning process into steps and provides evidence for each step.")

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
    elif llm_type == "Claude":
        api_key = st.sidebar.text_input("Enter Anthropic API Key", type="password")
        api_config = {
            "model_name": st.sidebar.selectbox(
                "Select Claude Model",
                ["claude-3-5-sonnet-20240620", "claude-3-opus-20240229", "claude-3-haiku-20240307"]
            )
        }
        use_openai_embed = False
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
    
    # Additional embedding options
    if llm_type == "OpenAI":
        embedding_api_key = api_key
    else:
        use_openai_embed = st.sidebar.checkbox("Use OpenAI for embeddings", value=False)
        if use_openai_embed:
            embedding_api_key = st.sidebar.text_input("Enter OpenAI API Key for embeddings", type="password")
        else:
            embedding_api_key = None
    
    # Document upload
    uploaded_file = st.file_uploader("Upload a document (PDF or TXT)", type=["pdf", "txt"])
    
    if uploaded_file is not None:
        document_text = load_document(uploaded_file)
        if document_text:
            with st.expander("Document Preview"):
                st.write(document_text[:500] + "...")
            
            # Chunk the document into pieces
            chunks = chunk_text(document_text, chunk_size=200)
            st.write(f"Document split into {len(chunks)} chunks.")
            
            # Load the local embedding model (used if not using OpenAI embeddings)
            embed_model = SentenceTransformer("all-MiniLM-L6-v2")
            
            # Create embeddings for all document chunks
            with st.spinner("Generating embeddings for document chunks..."):
                chunk_embeddings = get_chunk_embeddings(
                    chunks, 
                    embed_model, 
                    use_openai_embedding=use_openai_embed, 
                    openai_api_key=embedding_api_key,
                )
                if chunk_embeddings is None and use_openai_embed:
                    st.error("Failed to generate OpenAI embeddings. Please check your API key or try using the local embedding model.")
                    return
            
            # Input the question
            question = st.text_input("Enter your question about the document:")
            
            if st.button("Generate Answer"):
                if question.strip() == "":
                    st.error("Please enter a valid question.")
                elif not api_key:
                    st.error(f"Please provide a valid {llm_type} API key.")
                else:
                    st.info(f"Generating reasoning steps using {llm_type}...")
                    
                    # Generate reasoning steps
                    reasoning_steps = generate_reasoning_steps(
                        question, 
                        document_text, 
                        llm_type, 
                        api_key, 
                        api_config
                    )
                    
                    if not reasoning_steps:
                        st.error("Could not generate reasoning steps. Please check your API key and model selection.")
                        return
                    
                    # Process each reasoning step
                    step_answers = []
                    
                    for i, step in enumerate(reasoning_steps, 1):
                        st.subheader(f"Step {i}: {step}")
                        
                        # Find relevant chunks for this step
                        with st.spinner(f"Finding relevant chunks for step {i}..."):
                            relevant_chunks = vector_search_for_step(
                                step,
                                chunk_embeddings,
                                chunks,
                                embed_model,
                                top_k=3,
                                use_openai_embedding=use_openai_embed,
                                openai_api_key=embedding_api_key
                            )
                        
                        if not relevant_chunks:
                            st.write("No relevant chunks found for this step.")
                            step_answers.append((step, "No relevant information found."))
                            continue
                        
                        # Generate answer for this step using the relevant chunks
                        with st.spinner(f"Generating answer for step {i} using {llm_type}..."):
                            step_answer = answer_step_with_evidence(
                                step, 
                                relevant_chunks, 
                                llm_type, 
                                api_key, 
                                api_config
                            )
                            step_answers.append((step, step_answer))
                        
                        st.write("**Answer for this step:**")
                        st.write(step_answer)
                        
                        # Show evidence for this step
                        st.write("**Evidence:**")
                        for j, (chunk_text_str, score) in enumerate(relevant_chunks, 1):
                            # Call the reflection agent to extract sentence-level evidence
                            with st.spinner(f"Extracting evidence {j} using {llm_type}..."):
                                evidence_sentence = reflection_agent(
                                    step, 
                                    chunk_text_str, 
                                    llm_type, 
                                    api_key, 
                                    api_config
                                )
                            st.write(f"Evidence {j} (similarity score: {score:.2f}): {evidence_sentence}")
                    
                    # Generate final answer
                    st.subheader("Final Answer")
                    with st.spinner(f"Synthesizing final answer using {llm_type}..."):
                        final_answer = generate_final_answer(
                            question, 
                            step_answers, 
                            llm_type, 
                            api_key, 
                            api_config
                        )
                    st.write(final_answer)
                    
                    # Evaluate correctness of the answer
                    st.subheader("Correctness Evaluation")
                    with st.spinner(f"Evaluating answer correctness using {llm_type}..."):
                        correctness_score, reasoning = evaluate_correctness(
                            question, 
                            final_answer, 
                            document_text, 
                            llm_type, 
                            api_key, 
                            api_config
                        )
                    
                    # Display correctness score with color coding
                    score_color = "red" if correctness_score < 0.4 else "orange" if correctness_score < 0.7 else "green"
                    st.markdown(f"<h3 style='color: {score_color};'>Correctness Score: {correctness_score:.2f}</h3>", unsafe_allow_html=True)
                    
                    # Display score as a progress bar
                    st.progress(correctness_score)
                    
                    # Display reasoning for the score
                    st.subheader("Evaluation Reasoning")
                    st.write(reasoning)
        else:
            st.error("Could not extract text from the uploaded file.")

if __name__ == '__main__':
    main()