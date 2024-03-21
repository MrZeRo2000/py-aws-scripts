
$cmd = "aws sts assume-role-with-saml --principal-arn arn:aws:iam::488115367344:saml-provider/ADFS --role-arn arn:aws:iam::488115367344:role/ADFS-BI-Prod-DataDeveloper --saml-assertion file://C:/Users/r1525/saml-assert.txt"

Invoke-Expression -Command $cmd | Tee-Object -Variable res 

Write-Host "Result: $res"