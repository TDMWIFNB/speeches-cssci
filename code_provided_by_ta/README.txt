READ ME FIRST

Contents

1. Folder with PDF or docx versions of speeches from the Tweedekamer from 2024

2. tweedeData.ipynb 
(You can reuse this code, specifying a different year, to enlarge this dataset)
The script used to download the stenograph documents (#1) using the Tweedekamer's open data api. 
It is assumed that all of these documents were created from speech. However, there is no guarantee that all speeches are in this dataset, nor that all downloaded documents are useful speech.
This script takes a long time to run.

3. tweedeHEader.ipynb
(You can reuse this code, specifying a different year, to enlarge this dataset)
The script used to go through the downloaded documents to:
a) correct filetype errors; 
b) create a csv file with the following information: file name, year, first ten lines of text as a single string. This will give you basic information about what type of speech and the topic under consideration.
c) create a csv file that collects the same information (file name, year, first ten lines of text) about a specific type of speech. 
d) option to specify the speech type. For example, we used this collect just the "tweeminutendebat" - "A two-minute debate is a short debate in which each political party is given two minutes to speak. Such a short debate is used to conclude a topic that members of parliament have previously debated in a committee. During the two-minute debate, members of parliament can submit motions."
This script takes a long time to run.

3. tweede_2024.csv and tweede_2024tweeminutendebat.csv
The summary of the documents, created by tweedeHEader.ipynb, so that you can explore the data without reading all the document files.
