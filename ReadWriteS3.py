import json
import boto3


class ReadWriteS3:
    def __init__(self, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION):
        self.session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID,
                                     aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)

    def write_s3(self, backet, path, text):
        s3 = self.session.resource('s3')
        object = s3.Object(backet, path)
        object.put(Body=text)

    def read_s3_json(self, backet, path):
        s3 = self.session.resource('s3')

        content_object = s3.Object(backet,
                                   path)
        file_content = content_object.get()['Body'].read().decode('utf-8')
        json_content = json.loads(file_content)
        print(json_content)
        return json_content

    def finde_s3(self, backet, path):
        s3 = self.session.resource('s3')
        my_bucket = s3.Bucket(backet)

        files = my_bucket.objects.filter(Prefix=path)

        files = [obj.key for obj in sorted(files, key=lambda x: x.last_modified,
                                           reverse=True)]
        if len(files) > 0:
            print(files[0])
            return files[0]
        else:
            return 0
