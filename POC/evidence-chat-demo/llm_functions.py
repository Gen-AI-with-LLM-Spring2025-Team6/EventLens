import re
import json
import requests
import streamlit as st
from openai import OpenAI
import anthropic

def generate_openai_response(system_prompt, user_prompt, model_name, api_key, max_tokens=300, temperature=0.2):
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

def generate_claude_response(system_prompt, user_prompt, model_name, api_key, max_tokens=300, temperature=0.2):
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

def generate_llama_response(system_prompt, user_prompt, model_name, api_key, api_url, max_tokens=300, temperature=0.2):
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

def generate_llm_response(llm_type, system_prompt, user_prompt, api_key, api_config, max_tokens=300, temperature=0.2):
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

def generate_reasoning_steps(question, document_text, llm_type, api_key, api_config):
    """
    Generate reasoning steps for answering the question using the selected LLM.
    """
    system_prompt = (
        "You are an expert reasoning agent. Given a question and a document, "
        "break down the reasoning process into clear, logical steps that would be needed to answer the question. "
        "Output ONLY the numbered steps without additional explanation or the actual answer."
    )
    user_prompt = f"Document: {document_text[:1000]}...\n\nQuestion: {question}\n\nBreak down the reasoning process into steps:"
    
    output = generate_llm_response(
        llm_type, system_prompt, user_prompt, 
        api_key, api_config, max_tokens=300, temperature=0.2
    )
    
    if output:
        # Extract numbered steps
        steps = []
        for line in output.split('\n'):
            line = line.strip()
            if re.match(r'^\d+\.', line):
                step = re.sub(r'^\d+\.\s*', '', line)
                steps.append(step)
        
        return steps
    else:
        return []

def answer_step_with_evidence(step, relevant_chunks, llm_type, api_key, api_config):
    """
    Given a reasoning step and relevant chunks, generate an answer for that step.
    """
    # Combine chunks into context
    context = "\n\n".join([chunk for chunk, _ in relevant_chunks])
    
    system_prompt = (
        "You are an expert reasoning agent. Given a reasoning step and relevant context, "
        "provide a concise answer for that step based solely on the provided context. "
        "Your answer should be factual and directly tied to the information in the context."
    )
    user_prompt = f"Reasoning Step: {step}\n\nContext:\n{context}\n\nAnswer for this step:"
    
    return generate_llm_response(
        llm_type, system_prompt, user_prompt, 
        api_key, api_config, max_tokens=150, temperature=0.2
    ) or "Could not generate answer for this step."

def generate_final_answer(question, step_answers, llm_type, api_key, api_config):
    """
    Generate a final answer based on all the step answers.
    """
    steps_with_answers = "\n\n".join([f"Step: {step}\nAnswer: {answer}" 
                                     for step, answer in step_answers])
    
    system_prompt = (
        "You are an expert reasoning agent. Given a question and a series of reasoning steps with their answers, "
        "synthesize a final comprehensive answer to the original question."
    )
    user_prompt = f"Question: {question}\n\nReasoning Steps and Answers:\n{steps_with_answers}\n\nFinal Answer:"
    
    return generate_llm_response(
        llm_type, system_prompt, user_prompt, 
        api_key, api_config, max_tokens=250, temperature=0.2
    ) or "Could not generate a final answer."

def reflection_agent(step, text_segment, llm_type, api_key, api_config):
    """
    Uses the selected LLM with a reflection prompt to extract a sentence from the text_segment
    that serves as evidence for the given reasoning step.
    """
    system_prompt = (
        "You are an agent helping with the relevance examination. "
        "Your task is to pick the sentence-level content that could be used as evidence supporting the reasoning step given a text segment. "
        "You should not make up any sentence but directly extract the sentence from the segment."
    )
    user_prompt = (
        f"Reasoning Step: {step}\n"
        f"Text Segment: {text_segment}\n\n"
        "Please extract the sentence from the text segment that best supports the reasoning step. "
        "Return exactly one sentence from the text segment."
    )
    
    output = generate_llm_response(
        llm_type, system_prompt, user_prompt, 
        api_key, api_config, max_tokens=100, temperature=0.0
    )
    
    if output:
        # Extract the text following "Evidences:" if available
        if "Evidences:" in output:
            evidence = output.split("Evidences:")[-1].strip()
        else:
            evidence = output.strip()
        return evidence
    else:
        return "Could not extract evidence."

def evaluate_correctness(question, final_answer, document_text, llm_type, api_key, api_config):
    """
    Uses the selected LLM to evaluate the correctness of the final answer based on the question and document.
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
        f"Answer to evaluate: {final_answer}\n\n"
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