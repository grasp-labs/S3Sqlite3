from setuptools import setup

def long_description():
    with open('README.md', 'r') as file:
        return file.read()

setup(
    name='S3Sqlite3',
    version='0.1',
    packages=['s3_sqlite3'],
    url='https://grasplabs.no/',
    license='MIT License',
    author='GraspLabs',
    author_email='yuan@grasplabs.no',
    description='Accessor for Sqlite3 in AWS S3',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    python_requires='>=3.6.0',
    install_requires=[
        'boto3>=1.24.81',
        'apsw>=3.39.3.0'
    ],
    py_modules=[
        's3_sqlite3',
    ],
)
