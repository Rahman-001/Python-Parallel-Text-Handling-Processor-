import sqlite3
import re

class CoreLogic:
    def __init__(self, db_name="text_storage.db"):
        self.db_name = db_name
        self._init_db()

    def _init_db(self):
        """Builds the searchable list structure (Database)"""
        conn = sqlite3.connect(self.db_name)
        # We ensure the table exists with the right structure
        conn.execute('''CREATE TABLE IF NOT EXISTS text_data 
                        (content TEXT, score INTEGER, category TEXT)''')
        conn.close()

    def clean_text(self, text):
        """FIRST FILTER: Removes junk characters and patterns using regex"""
        # Remove HTML tags if any
        text = re.sub(r'<.*?>', '', str(text)) 
        # Remove special characters but keep spaces
        text = re.sub(r'[^a-zA-Z0-9\s]', '', text) 
        return text.strip()

    def advanced_scorer(self, text):
        """RULE CHECKER & SCORER: Weighted logic for feelings"""
        cleaned = self.clean_text(text).lower()
        
        # Rule definitions: Partner can expand this list easily
        patterns = {
            r'urgent|immediate|asap': -5, 
            r'excellent|perfect|great|happy': 5, 
            r'error|fail|broken|bad': -3,
            r'provisional|success': 2
        }
        
        score = 0
        for pattern, weight in patterns.items():
            if re.search(pattern, cleaned): 
                score += weight
        return score

    def run_search(self, keyword):
        """SEARCH CHECKER: Lets users find info in the database"""
        conn = sqlite3.connect(self.db_name)
        # Note: requires pandas installed if using read_sql_query
        import pandas as pd
        query = "SELECT * FROM text_data WHERE content LIKE ?"
        results = pd.read_sql_query(query, conn, params=(f'%{keyword}%',))
        conn.close()
        return results