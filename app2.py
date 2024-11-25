import streamlit as st
from elasticsearch import Elasticsearch
from tika import parser
import os

# Replace with your cloud configuration details
CLOUD_ID = "a542204ba92a41859df9e8b2aec8b595:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvJGE5MTI5ZjE4YTgyYzRjYThhOWQyOWVhYzExYmQ5NzI3JGVlYmQ5NjYzOGJhZTRhOGU5NDc5MTg2ZGI3NzhkNTNi"  # Provided in Elastic Cloud Console
API_KEY = "NzhvbVk1TUJFUUhVRU5Bd3AyZ2k6WElZMl92MVlRVmFrVUJsUDdwRnNrZw=="  # API Key generated in Elastic Cloud

# Connect to Elasticsearch Cloud
es = Elasticsearch(
    cloud_id=CLOUD_ID,
    api_key=API_KEY
)

# Function to extract text from a PDF
def extract_text_from_pdf(pdf_path):
    try:
        parsed = parser.from_file(pdf_path)
        return parsed['content']
    except Exception as e:
        return f"Error extracting text: {str(e)}"

# Function to index the PDF content
def index_pdf(pdf_path, index_name="pdf_search"):
    try:
        pdf_text = extract_text_from_pdf(pdf_path)
        if not pdf_text.strip():
            return "The PDF has no readable text to index."
        document = {
            "file_name": os.path.basename(pdf_path),
            "content": pdf_text
        }
        
        es.index(index=index_name, document=document)
        return "Indexed successfully!"
    except Exception as e:
        return f"Error indexing PDF: {str(e)}"

# Function to search for keywords in indexed PDFs
def search_pdf(keyword, index_name="pdf_search"):
    try:
        query = {
            "query": {
                "match": {
                    "content": keyword
                }
            },
            "highlight": {
                "fields": {
                    "content": {
                        "fragment_size": 150,  
                        "number_of_fragments": 3  
                    }
                }
            }
        }
        response = es.search(index=index_name, body=query)
        return response['hits']['hits']
    except Exception as e:
        return f"Error searching PDF: {str(e)}"

# Streamlit UI
st.title("PDF Searcher with Elasticsearch Cloud")

# File upload
uploaded_file = st.file_uploader("Upload a PDF", type="pdf")
if uploaded_file:
    file_path = os.path.join("uploads", uploaded_file.name)
    os.makedirs("uploads", exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    st.success(f"File '{uploaded_file.name}' uploaded successfully!")
    
    # Index the uploaded PDF
    status = index_pdf(file_path)
    st.write(status)

# Keyword search
keyword = st.text_input("Enter a keyword to search:")
if keyword:
    results = search_pdf(keyword)
    if isinstance(results, list) and results:
        st.write(f"Found {len(results)} result(s):")
        for result in results:
            st.write(f"**File:** {result['_source']['file_name']}")
            if "highlight" in result and "content" in result["highlight"]:
                st.write("**Matches:**")
                for fragment in result["highlight"]["content"]:
                    st.write(f"- {fragment}")
            else:
                st.write(f"**Snippet:** {result['_source']['content'][:200]}...")
    elif isinstance(results, str):
        st.write(results)  
    else:
        st.write("No results found.")
