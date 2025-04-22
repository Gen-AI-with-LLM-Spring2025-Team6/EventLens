import streamlit as st
from openai import OpenAI

def generate_openai_response(system_prompt, user_prompt, model_name, api_key, max_tokens=300, temperature=0.2):
    """
    Generate response using OpenAI models.
    
    Args:
        system_prompt (str): System instructions for the model
        user_prompt (str): User query or input
        model_name (str): Name of the OpenAI model to use
        api_key (str): OpenAI API key
        max_tokens (int): Maximum number of tokens in the response
        temperature (float): Temperature parameter for response generation
        
    Returns:
        str: Generated response or None if an error occurs
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