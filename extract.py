import os
from zipfile import ZipFile
from urllib.request import urlretrieve

def unzip_nested_zip(url, output_dir):
    # Check if the output directory exists
    if not os.path.exists(output_dir):
        # Download the zip file
        input_file = "./dropBoxFile.zip"
        urlretrieve(url, input_file)

        # Unzip the first level of the zip file
        with ZipFile(input_file) as zip_obj:
            zip_obj.extractall(output_dir)

        # Unzip any nested zip files
        extracted_dir = output_dir
        for file_name in os.listdir(extracted_dir):
            file_path = os.path.join(extracted_dir, file_name)
            if os.path.isfile(file_path) and file_path.endswith('.zip'):
                # Create a directory with the same name as the zip file (without the extension)
                unzip_dir = os.path.splitext(file_path)[0]
                os.makedirs(unzip_dir, exist_ok=True)

                # Unzip the file into the new directory
                with ZipFile(file_path) as zip_obj:
                    zip_obj.extractall(unzip_dir)
                    print(f"Unzipped: {file_path}")

# Specify the URL of the zip file and the output directory