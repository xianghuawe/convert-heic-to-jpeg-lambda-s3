mkdir -p convert-heic
pip install -r requirements.txt --target=./convert-heic
cd convert-heic
zip -r ../convert-heic.zip .
cd ..
zip convert-heic.zip lambda_function.py