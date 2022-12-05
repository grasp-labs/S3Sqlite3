# Deploy S3Sqlite3 as Aws Lambda Layer

## AWS Lambda

When building serverless applications, it is quite common to have code that is shared across Lambda functions. It can be your custom code, that is used by more than one function, or a standard library, that you add to simplify the implementation of your business logic.

We can put common components in a ZIP file and upload it as a Lambda Layer. The function code doesn’t need to be changed and can reference the libraries in the layer as it would normally do.
Layers can be versioned to manage updates, each version is immutable. When a version is deleted or permissions to use it are revoked, functions that used it previously will continue to work, but you won’t be able to create new ones.

![lambda layer](https://d2908q01vomqb2.cloudfront.net/0a57cb53ba59c46fc4b692527a38a87c78d84028/2020/06/23/Architecture2.gif)

Lambda functions can have up to five different lambda layers, with the total size of the Lambda deployment package with all layers unzipped not exceeding 250 MB.

Lambda layers won’t reduce your cold start times as such, but will increase the speed of deployments, ensure your lambdas are just your ‘code’, enables shared code and asset reuse, and ensures you don’t hit the dreaded ‘CodeStorageExceeded’ error.

## Deploy S3Sqlite3 

Using the [serverless framework](https://www.serverless.com) we can then ensure that this layer is deployed to AWS.

Details regarding the deployment can be found [here](https://www.serverless.com/framework/docs/providers/aws/guide/layers).

After deployment, the layer: `arn:aws:lambda:eu-north-1:999125116186:layer:GraspS3Sqlite3Layer:1` is ready to be consumed.
