
### Code from an Azure Autonmation Runbook
### Every night, we spun up a cluster, whose configs we defined ahead of time in a template;
### We then ran some hdfs distcp commands and shut down the cluster 

Set-AzureRmContext -TenantId "some-tenant-id"

$storageAccountName = "myStorageAccountName"
$storageAccountKey = Get-AzureKeyVaultSecret -VaultName 'myKeyVault' -Name 'myStorageAccountKey'
$resourceGroupName = 'myResourceGroup'

#get a storage account context for the template URI 
$storageContext = New-AzureStorageContext -StorageAccountName $storageAccountName -StorageAccountKey $storageAccountKey
$templateuri = New-AzureStorageBlobSASToken -Container admin -Blob template.json -Permission r -Context $storageContext `
  -ExpiryTime (Get-Date).AddHours(2.0) -FullUri

#get the cluster password
$SshPasswordUnsecured = Get-AzureKeyVaultSecret -VaultName 'myKeyVault' -Name 'mySshPassword'
$SshPassword = ConvertTo-SecureString -String $SshPasswordUnsecured -AsPlainText -Force

#get the cluster identity certificate
$secretRetrieved = Get-AzureKeyVaultSecret -VaultName 'myKeyVault' -Name 'HadoopClusterCertificate'
$Cert = $secretRetrieved.SecretValueText
$secureCert = ConvertTo-SecureString -String $Cert -AsPlainText -Force
$certPasswordSecureString = $SSHpass

#spin up the cluster
$sw = [Diagnostics.Stopwatch]::StartNew()
New-AzureRmResourceGroupDeployment `
    -ResourceGroupName $resourceGroupName `
    -TemplateUri $templateuri `
    -identityCertificate $secureCert `
    -identityCertificatePassword $certPasswordSecureString `
    -clusterName 'myHadoopCluster' `
    -clusterLoginPassword $SshPassword `
    -sshPassword $SshPassword

$sw.Stop()
$sw.Elapsed

#get the script to run on the cluster
$distcpuri = New-AzureStorageBlobSASToken -Container hdinsightscripts -Blob distcps3todatalake.sh -Permission r -Context $storageContext `
  -ExpiryTime (Get-Date).AddHours(2.0) -FullUri

#run the distcp commands
Submit-AzureRmHDInsightScriptAction `
            -ClusterName "myHadoopCluster" `
            -Name "distcps3todatalake" `
            -Uri $distcpuri `
            -NodeTypes Worker -PersistOnSuccess


#remove the cluster
Remove-AzureRmHDInsightCluster -ClusterName 'myHadoopCluster'

