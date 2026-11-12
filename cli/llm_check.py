"""Diagnose LLM connectivity step by step:  python -m cli.llm_check"""
from engine.llm_check import diagnose, summary




if __name__ == "__main__":
    print(summary(diagnose()))

    import os, sys; sys.path.insert(0,'.'); import dotenv; print('dotenv path exists:', dotenv.find_dotenv(usecwd=True)); print('dotenv .env keys:', dotenv.dotenv_values('.env').get('OPENAI_API_KEY')); print('env var now:', os.environ.get('OPENAI_API_KEY')); print('same?', dotenv.dotenv_values('.env').get('OPENAI_API_KEY') == os.environ.get('OPENAI_API_KEY'))
