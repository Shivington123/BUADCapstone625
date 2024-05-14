import streamlit as st
from milestone3 import download_extract_zip, extract_csv_from_dropbox, process_files
import pandas as pd
import base64

def main():
    st.title("Milestone 3")

    # Text input for user to provide the URL of the test data
    userURL = st.text_input('Enter URL of Test Data')

    if st.button('Process Data'):
        if userURL:
            # Download and process the data
            outputDir = "dropbox"
            inputFile = "./dropBoxFile.zip"
            url = "https://www.dropbox.com/sh/rlrv8z10ahyhl8l/AADQs5cFZKxKHUVlyNeAkGjua?dl=1"

            # Download and extract the ZIP file from the user provided URL
            download_extract_zip(url, inputFile, outputDir)

            # Extract the CSV file from the downloaded ZIP file
            extracted_term = extract_csv_from_dropbox(userURL)

            # Process the extracted CSV file
            process_files(extracted_term)

            # Read the processed data into a DataFrame
            data = pd.read_csv(extracted_term)

            # Convert DataFrame to CSV format
            csv = data.to_csv(index=False)

            # Encode CSV data to base64 for download
            b64 = base64.b64encode(csv.encode()).decode()

            # Provide a download link for the processed CSV file
            st.markdown(get_download_link(b64, filename=extracted_term), unsafe_allow_html=True)
        else:
            # Display a warning message if no URL is provided
            st.warning("Please enter a URL before processing.")

def get_download_link(content, filename='file.csv'):
    """Generate a download link for a given content."""
    href = f'<a href="data:file/csv;base64,{content}" download="{filename}">Download CSV File</a>'
    return href

if __name__ == '__main__':
    main()
