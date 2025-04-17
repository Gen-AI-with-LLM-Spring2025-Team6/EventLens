from openai import OpenAI

def generate_openai_response(system_prompt, user_prompt, model_name, api_key, max_tokens=500, temperature=0.3):
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
        return f"OpenAI Error: {e}"