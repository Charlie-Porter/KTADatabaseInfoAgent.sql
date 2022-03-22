# KTADatabaseInfoAgent.sql

This script returns SQL server information to help troubleshoot performance and data issues.

It is essential to read and follow these instructions rather than just running the script as-is.

- Download the Database Information Script from here.
- Requires VIEW SERVER STATE permission on the server
- Replace all [TotalAgility] with name of your TotalAgility database eg [TotalAgility] with [TotalAgility_UAT] or for OPMT replace all [TotalAgility] with the name of your tenant main database eg [TotalAgility_Tenant]
- Replace all [TotalAgility_Documents] with name of your TotalAgility Documents database eg [TotalAgility_Documents] with [TotalAgility_Documents_UAT] or for OPMT replace all [TotalAgility_Documents] with the name of your tenant Documents database eg [TotalAgility_Tenant]
- Replace all [TotalAgility_FinishedJobs] with name of your TotalAgility Finished jobs database eg [TotalAgility_FinishedJobs] with [TotalAgility_FinishedJobs_UAT]. For OPMT replace all [TotalAgility_FinishedJobs] with the name of your tenant FinishedJobs database eg [TotalAgility_Tenant]
- For OPMT, replace all [dbo] with [live] or [dev] as per the OPMT deployment type.
- Then right-click on the query window, Results To, and select "Results to file"
- Execute
- Once the queries complete, please attach the results RPT file to your Kofax Support case  
