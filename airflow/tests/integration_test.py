import pytest
import boto3
import psycopg2

def test_real_redshift_connection(real_redshift_connection):
    cursor = None

    try:
        cursor = real_redshift_connection.cursor()
        cursor.execute("SELECT 1;")
        result= cursor.fetchone()

        assert result[0] == 1
    except psycopg2.Error as e :
        pytest.fail(f"Databse query failed: {e} ")

    finally:
        if cursor is not None:
            cursor.close()

#Verify that your local pipeline can write data to cloud storage like Amazon S3.
def test_upload_to_s3(s3_client):
    bucket = "s3-test1-data-bucket"
    file_name = "test_file.csv"

    try:

        s3_client.upload_file(file_name, bucket ,file_name)

        response = s3_client.list_objects_v2(Bucket=bucket)

        files = [obj["Key"] for obj in response.get("Contents", [])]

        assert file_name in files
    except Exception as e:
        pytest.fail(f"failed to upload file {e}")
    finally:
        # Cleanup: delete uploaded file
        try:
            s3_client.delete_object(Bucket=bucket, Key=file_name)
        except Exception:
            pass
