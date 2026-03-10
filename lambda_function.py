import json
import boto3
from PIL import Image
import pillow_heif
import os
import gc

def lambda_handler(event, context):
    s3_client = boto3.client("s3")

    # Get bucket and key from the S3 event
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]

    # Get object metadata to check Content-Type
    head_response = s3_client.head_object(Bucket=bucket, Key=key)
    content_type = head_response.get('ContentType', '')
    
    # Check if Content-Type already indicates HEIC or WebP
    if content_type in ['image/heic', 'image/webp']:
        print(f"Content-Type indicates {content_type}, proceeding with conversion")
        needs_conversion = True
        conversion_type = content_type.split('/')[1]
    else:
        # Download only the first 12 bytes to check file signature
        range_response = s3_client.get_object(
            Bucket=bucket, 
            Key=key,
            Range='bytes=0-11'  # Only first 12 bytes
        )
        file_header = range_response["Body"].read()
        
        # Check file signatures
        needs_conversion, conversion_type = check_file_signature(file_header)
        
        if not needs_conversion:
            print(f"File {key} does not need conversion. Content-Type: {content_type}")
            return

    try:
        # Download the full file to /tmp (reduce memory usage)
        temp_input = f"/tmp/{os.path.basename(key)}"
        s3_client.download_file(bucket, key, temp_input)

        # Convert based on detected type
        if conversion_type == 'heic':
            # Convert HEIC to image
            heif_file = pillow_heif.read_heif(temp_input)
            image = Image.frombytes(
                heif_file.mode,
                heif_file.size,
                heif_file.data,
                "raw",
            )
        else:  # webp
            # Convert WebP to image
            image = Image.open(temp_input)

        # Convert RGBA to RGB if necessary
        if image.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', image.size, (255, 255, 255))
            if image.mode == 'P':
                image = image.convert('RGBA')
            if image.mode in ('RGBA', 'LA'):
                background.paste(image, mask=image.split()[-1])
                image = background

        # Save to /tmp instead of memory
        temp_output = temp_input.rsplit('.', 1)[0] + '.png'
        image.save(temp_output, format="PNG")
        
        # Close and free memory
        image.close()
        del image
        gc.collect()

        # Upload PNG to S3
        with open(temp_output, 'rb') as f:
            s3_client.upload_fileobj(
                f,
                bucket,
                key,
                ExtraArgs={"ContentType": "image/png"},
            )

        # Clean up temp files
        if os.path.exists(temp_input):
            os.remove(temp_input)
        if os.path.exists(temp_output):
            os.remove(temp_output)
        gc.collect()

        print(f"Successfully converted {key} to PNG")
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": f"Successfully converted {key}",
                    "input": event,
                }
            ),
        }

    except Exception as e:
        print(f"Error converting {key}: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps(
                {"message": f"Error converting file: {str(e)}", "input": event}
            ),
        }

def check_file_signature(header):
    """Check file signature to determine if it's HEIC or WebP"""
    # HEIC file signature (ISOBMFF format)
    if len(header) >= 12 and header[4:8] == b'ftyp':
        # Check for HEIC brands
        brands = [b'heic', b'heix', b'hevc', b'hevx']
        for brand in brands:
            if len(header) >= 12 and header[8:12] == brand:
                print("Detected HEIC file via signature")
                return True, 'heic'
    
    # WebP file signature
    if len(header) >= 12 and header[:4] == b'RIFF' and header[8:12] == b'WEBP':
        print("Detected WebP file via signature")
        return True, 'webp'
    
    return False, None