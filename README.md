# Introduction 
The files in this repo are used to migrate and sync data between OneBill and other external DBs.

# Getting Started
Create you own .env file (these are ignored in the git commits as they contain confidential iformation)
Add the following variables with your unique values:

MySQL Connections:
DB_USERNAME=
DB_PASSWORD=
DB_HOST=
DB_PORT= (optional; default MySQL port is used when blank)

OneBill Connections:
CLIENT_ID=
CLIENT_SECRET=
API_USERNAME=api.usersbx
API_PASSWORD=
ONEBILL_BASE_URL=

OneBill Configurations:
DELETION_PROXY_ACCOUNT_NUMBER=
CREATION_PROXY_ACCOUNT_NUMBER=

PROD Dynamics Connections:
CRM_TENANT_ID=
CRM_CLIENT_ID=
CRM_CLIENT_SECRET=
CRM_ENVIRONMENT_URL=

DEV Dynamics Connections:
DEV_CRM_TENANT_ID=
DEV_CRM_CLIENT_ID=
DEV_CRM_CLIENT_SECRET=
DEV_CRM_ENVIRONMENT_URL=

The variable names must be identical as these are called within the scripts.

To migrate customers into OneBill use the 'OneBill Customer Migration' file. As simple as just hitting 'Run All', but you might just want to make the dataframes smaller the first couple run throughs.
Close Partner Accounts closes all accounts under a parter. Partners can't be closed when there is an active account under it.
Close vBill Accounts I run after the migration file to close any 'Deactivated' accounts that were in vBill.


# Build and Test
TODO: Describe and show how to build your code and run the tests. 

# Contribute
TODO: Explain how other users and developers can contribute to make your code better. 

If you want to learn more about creating good readme files then refer the following [guidelines](https://docs.microsoft.com/en-us/azure/devops/repos/git/create-a-readme?view=azure-devops). You can also seek inspiration from the below readme files:
- [ASP.NET Core](https://github.com/aspnet/Home)
- [Visual Studio Code](https://github.com/Microsoft/vscode)
- [Chakra Core](https://github.com/Microsoft/ChakraCore)