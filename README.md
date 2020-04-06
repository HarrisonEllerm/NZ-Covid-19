# NZ-Covid-19

This script downloads data provided by the Ministry of Health related to COVID-19 in New Zealand and augments
it with geo-location data. It loads then loads this data into dataframes which are then imported into Power BI
as a way to hack up a quick dashboard.

This mini-project was used as a way to experiment with importing data into Power-BI via a python script. It is not
intended to be used as an official source of information related to COVID-19.


### Initial Dashboard Mock Up

![Dashboard Example](https://github.com/HarrisonEllerm/NZ-Covid-19/blob/master/Dashboard_Ex.PNG?raw=True)

Ministry of Health Link:

https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19-current-situation/covid-19-current-cases

### Findings:

It is relatively easy to implement a python script for pumping data into a dashboard. However, there is the occasional
oddity that can cause confusion. For example, Power-BI doesn't mind the use of the os module for joining paths, however
if you use the os module to test to see if a file exists Power-BI fails to import the data resulting in the somewhat
confusing error message below:

```
DataSource.Error: ADO.NET: A problem occurred while processing your Python script

Here are the technical details:
Running the Python script encountered the following error:
Incorrect function.

Details:
    DataSourceKind=Python
    DataSourcePath=Python
    Message=A problem occurred while processing your Python script.

Here are the technical details:
    Running the Python script encountered the following error:

Incorrect function.
    ErrorCode=-2147467259
    ExceptionType=Microsoft.PowerBI.Scripting.Python.Exceptions.PythonUnexpectedException
```
Caused by:
```python
    if os.path.exists(file_loc):
        print(f'>> Latest data has already been downloaded, exiting...')
        exit(1)
    else:
        new_file = open(file_loc, 'wb')
        response = get(from_url)
        new_file.write(response.content)
        return file_loc
```
This somewhat limits the ability to check to see if folders exist and create them if they don't (for holding resources
used within the script). I got around this by creating the folders manually. The error message also doesn't provide
very useful information, limiting the programmer to manual debugging.
