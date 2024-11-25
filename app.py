import streamlit as st
from elasticsearch import Elasticsearch
from tika import parser
import os


es = Elasticsearch("http://localhost:9200")


def extract_text_from_pdf(pdf_path):
    try:
        parsed = parser.from_file(pdf_path)
        return parsed['content']
    except Exception as e:
        return f"Error extracting text: {str(e)}"


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

st.title("PDF Searcher with Elasticsearch")


uploaded_file = st.file_uploader("Upload a PDF", type="pdf")
if uploaded_file:
    file_path = os.path.join("uploads", uploaded_file.name)
    os.makedirs("uploads", exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(uploaded_file.getbuffer())
    st.success(f"File '{uploaded_file.name}' uploaded successfully!")
    
    
    status = index_pdf(file_path)
    st.write(status)


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
