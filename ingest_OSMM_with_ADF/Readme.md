# Importing Ordnance Survey MasterMap data with Azure Data Factory

Implementations
- [Using DataPackages via the OS Download API](#using-datapackages-via-the-os-download-api)

## Using DataPackages via the OS Download API
This example relates to downloading `DataPackages`, which represent custom datasets served on request to members of the Public Sector Geospatial Agreement (PSGA). 

- [About the OS Download API](https://osdatahub.os.uk/docs/downloads/overview)
- [About DataPackages](https://www.ordnancesurvey.co.uk/business-government/public-sector-geospatial-agreement/public-sector-premium-data)

👉 ***Goal***: Create a parameterised pipeline that can pull in data packages directly to your Azure blob storage or data lake in a repeatable and parallelised way, implementing optional throttling of the API calls made to the OS Data Hub. 💪

### **Pre-requisites**

1. request a DataPackage via the PSGA, and have the data package id and data package version number to hand, as well as your API key for the OS data hub.
2. Provision or identify the following resources in your Azure subscription:
   - [Azure Storage Account](https://docs.microsoft.com/en-us/azure/storage/common/storage-account-create) (this can optionally have [hierarchical namespace enabled](https://docs.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-namespace) to make it an Azure Datalake gen2) 
   - An [Azure Data Factory](https://docs.microsoft.com/en-us/azure/data-factory/quickstart-create-data-factory-portal) must be available. If provisioning a new Data Factory, chose to skip setting up the git integration for now. 

### **Setup**
- [Option 1](#setup-option-1-new-azure-data-factory): New Azure Data Factory
- [Option 2](#setup-option-2-existing-azure-data-factory-with-existing-git-integration): Existing Azure Data Factory, with existing git integration
- [Option 3](#setup-option-3-existing-azure-data-factory-not-integrated-with-source-control): Existing Azure Data Factory, not integrated with source control

#### **Setup Option 1**: New Azure Data Factory
Start by forking this repo into your own git account.

From the ADF portal (https://adf.azure.com), [configure git integration](https://docs.microsoft.com/en-us/azure/data-factory/source-control#configuration-method-3-management-hub) with the following parameters:
- Repository Type: `GitHub`
- GitHub account: `<your github user name or organisation>`
- Repository name: `work_with_OrdnanceSurvey_data`
- Collaboration branch: `main`
- Publish branch: `adf_publish` *
- Root folder: `/ingest_OSMM_with_ADF/adf`

\* *Note: the `adf_publish` branch will be an orphan branch only containing arm templates that can be used to deploy the solution into other environments*

#### **Setup Option 2**: Existing Azure Data Factory, with existing git integration
Create a new branch in your existing repo to explore this example.

Using the Github portal or a local clones of the relevant repos, manually copy the files in the `/ingest_OSMM_with_ADF/adf` folder of this repo into the same folder structure in your existing repo. Your root folder may be in a different location, for example `/adf` or `/` - that is fine.

Open the Azure Data Factory portal, and refresh, then then switch to the new branch created above. 

#### **Setup Option 3**: Existing Azure Data Factory, not integrated with source control

You can either follow Option 1 above, and allow all existing content of your data factory to be imported into your forked repo. 
> :warning: Make sure to remove any hard coded credentials in your existing content before doing this. 

Alternatively, manually create the assets described below, and use the code view (button `{}`, top right corner of the authoring window) to replicate the examples from this repo. 

### **Configure Linked Services**
Navigate to the `Manage` tab within Data Factory, and delete the `azuregigdatalake` linked service, and replace it with an entry linking your own storage account. ([Link a blob storage account](https://docs.microsoft.com/en-us/azure/data-factory/connector-azure-blob-storage?tabs=data-factory#create-an-azure-blob-storage-linked-service-using-ui) or [Link an Azure Data lake Storage Gen 2](https://docs.microsoft.com/en-us/azure/data-factory/connector-azure-data-lake-storage?tabs=data-factory#create-an-azure-data-lake-storage-gen2-linked-service-using-ui))

Then find the dataset representing the target location for the ingest (see Dataset 3 under Assets below) and ensure it uses your new linked service. Ensure you create a `bronze` container or equivalent destination in your datalake, and update the path in the dataset accordingly.

### **Execute the Pipeline**
:tada: You're ready to run the pipeline: click the debug button at the top of the pipeline canvas to [execute a debug run](https://docs.microsoft.com/en-us/azure/data-factory/iterative-development-debugging?tabs=data-factory#monitoring-debug-runs). A panel will pop up asking for the Data Package id, the Data Package version, and your api key for the OS Data Hub.

> In this sample, the ingest is limited to 10 gzipped archives specified in the data package. Review the detail below to adapt this to your needs. 

### **About the assets defined by this sample**

This sample defines one [pipeline](./adf/pipeline) and three [datasets](./adf/dataset).

<p align="center"><img src=img/assets.png width=250 /></p>

**Dataset 1:** The [PackageIndex](./adf/dataset/PackageIndex.json) dataset represents the data retrieved by the initial API call (GET request) to the OS Download API. It is a http dataset of type `Json` and we will use this in a `Lookup` activity in the pipeline later on.

<p align="center"><img src=img/PackageIndex.png width=650/></p>

The package id and package version are parameterised in the Relative URL field as follows:
```
@concat('downloads/v1/dataPackages/',dataset().package_id,'/versions/',dataset().package_version)
```

To support this, the Parameters tab for the dataset needs to be configured as follows:

<p align="center"><img src=img/PackageIndexParams.png width=650/></p>

**Dataset 2:** The [OSMM_source_file](adf/dataset/OSMM_source_file.json) dataset represents a single file to be downloaded as part of the package. Since the files are zipped `gz` files served by a http server, it is a http dataset of type `Binary`.

<p align="center"><img src=img/OSMM_source_file.png width=650/></p>

The relative url is parameterised as above, but also with the specific filename. Later, we will loop through the list of filenames that make up a package in a `ForEach` activity in the pipeline.

```
@concat('downloads/v1/dataPackages/',dataset().package_id,'/versions/',dataset().package_version,'/downloads?fileName=',dataset().filename)
```
To support this, we have an additional parameter configured on this dataset.

<p align="center"><img src=img/OSMM_source_file_params.png width=650/></p>

**Dataset 3:** The [OSMM_bronze_gml_gz](./adf/dataset/OSMM_bronze_gml_gz) dataset represents the target location for our pipeline, and is configured as an Azure Data Lake Gen2 dataset Since we will be copying the files 'as is', this is also of type `Binary`, meaning any copy process will not attempt to read any data schema within the files. 

<p align="center"><img src=img/OSMM_bronze_gml_gz.png width=650/></p>

The target path within the `bronze` container of the data lake is parameterised as follows:

```
@concat('gz/OSMM/v1/dataPackages/',dataset().package_id,'/version/',dataset().package_version)
```
requiring the same parameters to be configured on the dataset as in the case of the source dataset above.

<p align="center"><img src=img/OSMM_source_file_params.png width=500/></p>

### **The Pipeline**

<p align="center"><img src=img/Pipeline.png width=650/></p>

At pipeline level, we configure the following parameters:

<p align="center"><img src=img/pipeline_parameters.png width=500/></p>

and the following variables, all of which will be used in the activities outlined below.

<p align="center"><img src=img/Pipeline_variables.png width=500/></p>



#### **"Lookup" activity**
The lookup activity fetches the initial response from the OS Download API:

<p align="center"><img src=img/Lookup.png width=650/></p>

As we are expecting a single json object to be returned, we also tick the box `First row only`.

Since the PackageIndex dataset is parameterised, we have to supply values for `package_id` and `package_version` here. Rather than hardcoding these, we have parameterised the pipeline itself as shown above, so that these parameters need to be supplied on pipeline invokation. 

In the screenshot above, we supply:

```py
# package_id: 
@pipeline().parameters.data_package
# package_version: 
@pipeline().parameters.data_package_version

# Additional headers follow the format "<key>:<value>"
@concat('key:',pipeline().parameters.api_key)
```

#### **"Set Variable" activity**
The call to the package index by the lookup activity returns a json object. By looking at the output of this step in a debug run, we can see the structure of the returned object.

To view the output of an activity, hover over the entry in the [debug run log](https://docs.microsoft.com/en-us/azure/data-factory/iterative-development-debugging?tabs=data-factory#monitoring-debug-runs), accessible either via the output tab on the pipeline, or via the monitor pane in ADF:

<p align="center"><img src=img/debug_run_output.png width=700/></p>

As a result of the `first row only` setting on the lookup activity, the structure of the object returned looks as follows:

<p align="center"><img src=img/lookup_object.png width=600/></p>

In the Set Variable activity, named `set array download_urls`, we therefore set:

```py
# Name: (variable needs to be configured at pipeline level as an array, as show above)
download_urls
# Value:
@activity('Lookup Package Index').output.firstRow.downloads
```

#### **Set Variable** activity 2 [optional]

Since the array of download urls can be very large, we set another variable with a small sample of these, so that we can iteratively develop the rest of the pipeline using the small sample only.

In the second Set Variable activity, named `set array download_urls_sample`, we therefore set:

```py
# Name: (variable needs to be configured at pipeline level as an array, as show above)
download_urls_sample
# Value:
@take(variables('download_urls'),10)
```

#### **"ForEach" Loop** 

Since the OS Data Hub implements a rate limit, the implementation here enables the throttling of parallel requests to the API through a combination of parallel batch size and wait time between batches. 

> *Note: With this throttling implementation, it is possible to throttle to 50 calls per second or slower, as the minimum value of a wait activity is 1s, and the maximum batch size setting in a ForEach activity is 50.*

In this example, we set the batch size to 5, giving us 2 batches of downloads in our sample of 10 urls. 

<p align="center"><img src=img/Loop.png width=450/></p>

Setting `Items` to the array stored in the `download_urls_sample` variable means that within the `ForEach` loop, each object in the array will be accessible with `@item()`.

We know from reviewing the output of the lookup activity above that each of these objects has the following structure:
```json
 {
    "fileName": "5436275-NJ6050.gz",
    "url": "https://api.os.uk/downloads/v1/dataPackages/0040154231/versions/5500114/downloads?fileName=5436275-NJ6050.gz",
    "size": 2833612,
    "md5": "2a0605f83156824e8a03c8fe1b1e6102"
},
```

As the `sequential` box is not ticked, the inner activities (call to download endpoint and a 1 second wait) will be executed for 5 files simultaneously:

<p align="center"><img src=img/loop_inner_activities.png width=450/></p>

#### **"Copy" activity**

The ***source*** of the copy activity within the ForEach loop supplies both pipeline parameters specifying `package_id` and `package_version`, and the `fileName` from each `@item()` to construct the relative url that we have already parameterised in the dataset as outlined [above](#Assets).

> *Note: we could also have parameterised the dataset differently so as to supply the entire relative url by doing string manipuation of the `@item().url` field to remove the base url.*

<p align="center"><img src=img/download_source.png width=650/></p>

```py
# package_id: 
@pipeline().parameters.data_package
# package_version: 
@pipeline().parameters.data_package_version
# filename:
@item().fileName

# Additional headers follow the format "<key>:<value>"
@concat('key:',pipeline().parameters.api_key)
```

The ***sink*** of the copy activity supplies the same parameters so that the parameterised target data path in the datalake can be constructed within the dataset:

<p align="center"><img src=img/download_sink.png width=650/></p>


### **Pipeline Execution**

When triggering the pipeline, for example with the `Debug` button, the api_key has to be supplied, ensuring that the key is not persisted in the [code base of the ADF in the linked git repo](./adf).

Here is an example of a throttled run, copying batches of 5 files simultaenously with a wait time of 5 seconds.

<p align="center"><img src=img/debug_run_throttled.png width=650/></p>
