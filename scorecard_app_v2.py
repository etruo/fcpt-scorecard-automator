import os
import tempfile
import json
from pathlib import Path
from io import BytesIO
from datetime import datetime
import openpyxl
import boto3
from botocore.exceptions import ClientError

import streamlit as st
from openai import OpenAI
import pandas as pd
from typing import Dict, Optional

import extractor
from build_scorecard import build_scorecard

# Initialize session state for history
if "history" not in st.session_state:
    st.session_state.history = []

# Initialize session state for authentication
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False

def get_template_from_s3() -> BytesIO:
    """Get the template file from S3."""
    try:
        # Get configuration
        bucket = os.getenv('S3_BUCKET_NAME')
        template_key = os.getenv('TEMPLATE_S3_KEY', 'templates/Scorecard - Blank v1 streamlit.xlsx')
        
        if st.session_state.get('debug_mode'):
            st.write("S3 Configuration:")
            st.write({
                "Bucket": bucket,
                "Template Key": template_key,
                "AWS Access Key ID": f"{os.getenv('AWS_ACCESS_KEY_ID', '')[:5]}..." if os.getenv('AWS_ACCESS_KEY_ID') else "Not set"
            })

        s3 = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        
        # List contents of bucket to verify access
        if st.session_state.get('debug_mode'):
            try:
                response = s3.list_objects_v2(Bucket=bucket, Prefix='templates/')
                st.write("Files in templates folder:")
                for obj in response.get('Contents', []):
                    st.write(f"- {obj['Key']}")
            except Exception as e:
                st.write(f"Error listing bucket contents: {str(e)}")
        
        # Download template to memory
        template_obj = BytesIO()
        s3.download_fileobj(bucket, template_key, template_obj)
        template_obj.seek(0)  # Reset file pointer to beginning
        return template_obj
    except ClientError as e:
        error_msg = f"Error downloading template: {str(e)}"
        if "404" in str(e):
            error_msg += "\nPlease verify the template file exists in the correct location in S3"
        st.error(error_msg)
        raise

def check_password():
    """Returns `True` if the user had the correct password."""
    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == os.getenv("ADMIN_PASSWORD", "goirish"):
            st.session_state["authenticated"] = True
            return True
        else:
            st.session_state["authenticated"] = False
            st.error("😕 Password incorrect")
            return False

    if not st.session_state.get("authenticated"):
        # First run, show input for password
        st.text_input(
            "Please enter the password", 
            type="password", 
            on_change=password_entered, 
            key="password"
        )
        return False
    
    return True

# Page configuration
st.set_page_config(
    page_title="FCPT Scorecard Automator",
    page_icon="🏢",
    layout="centered",
    initial_sidebar_state="expanded"
)

if check_password():
    st.title("FCPT Scorecard Automator v2")

    # ---------- sidebar: API key, settings & history ------------------------------------------------
    with st.sidebar:
        st.markdown("### 🔑 OpenAI API key")
        if "OPENAI_API_KEY" not in os.environ:
            user_key = st.text_input("Paste your key", type="password")
            if user_key:
                os.environ["OPENAI_API_KEY"] = user_key
                st.success("Key stored for this session")
        
        st.markdown("### ⚙️ Settings")
        debug_mode = st.checkbox("Enable debug mode", value=False, help="Show detailed processing logs")
        if debug_mode:
            os.environ["DEBUG"] = "1"
        else:
            os.environ.pop("DEBUG", None)
        
        # Display history
        if st.session_state.history:
            st.markdown("### 📚 Previous Models")
            for idx, item in enumerate(st.session_state.history):
                with st.expander(f"#{idx + 1}: {item['property_name']} ({item['date']})"):
                    st.json(item['data'])
                    if 'excel_data' in item:
                        st.download_button(
                            "📥 Download This Model",
                            data=item['excel_data'],
                            file_name=item['filename'],
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )

    # ---------- input choice ----------------------------------------------------
    mode = st.radio("Choose input type", ("E-mail text", "Offering Memorandum PDF"))
    txt, pdf = "", None

    if mode == "E-mail text":
        txt = st.text_area("✉️ Paste e-mail body", height=250)
    else:
        pdf = st.file_uploader("📄 Upload OM (PDF)", type=["pdf"])
        if pdf:
            st.info("Using template from secure storage")

    run = st.button("🚀 Extract + Build", disabled=(not txt and not pdf))

    if run:
        try:
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

            with st.spinner("Extracting data and building model..."):
                # Get template from S3
                template_obj = get_template_from_s3()
                
                # Save template temporarily
                with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp_template:
                    tmp_template.write(template_obj.getvalue())
                    template_path = tmp_template.name

                # Process based on input mode
                if mode == "E-mail text":
                    fields, excel_path = build_scorecard(
                        txt,
                        template_path=template_path,
                        client=client
                    )
                    src_name = "email"
                else:
                    tmp = Path(tempfile.gettempdir()) / "upload.pdf"
                    tmp.write_bytes(pdf.getvalue())
                    fields, excel_path = build_scorecard(
                        tmp,
                        template_path=template_path,
                        client=client
                    )
                    src_name = Path(pdf.name).stem

                # Clean up temporary template
                os.unlink(template_path)

                # Show extraction results
                with st.expander("🔍 Extracted Data", expanded=True):
                    st.json(fields)

                # Get tenant name and location info
                tenant_name = fields.get("Current Tenant", "Unnamed Tenant")
                address = fields.get("Address", {})
                city = address.get("City", "")
                state = address.get("State", "")
                location_str = f"({city}, {state})" if city and state else ""
                
                current_date = datetime.now().strftime("%m.%d.%y")
                
                # Create standardized filename
                standardized_filename = f"Automated Scorecard {tenant_name} {location_str} {current_date} v1.xlsx"
                
                if debug_mode:
                    st.write("File Information:")
                    st.write({
                        "Tenant Name": tenant_name,
                        "Location": location_str,
                        "Date": current_date,
                        "Final Filename": standardized_filename
                    })

                # Read the generated Excel file
                with open(excel_path, "rb") as fh:
                    bytes_xlsx = fh.read()

                st.success("✅ Model successfully built!")
                
                # Add to session history
                st.session_state.history.append({
                    'property_name': tenant_name,
                    'date': current_date,
                    'data': fields,
                    'excel_data': bytes_xlsx,
                    'filename': standardized_filename
                })

                # Create download button
                st.download_button(
                    "📥 Download Excel Model",
                    data=bytes_xlsx,
                    file_name=standardized_filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

                # Clean up temporary files
                if mode == "Offering Memorandum PDF":
                    os.unlink(tmp)
                os.unlink(excel_path)

        except Exception as e:
            st.error(f"Error: {str(e)}")
            if debug_mode:
                st.exception(e) 