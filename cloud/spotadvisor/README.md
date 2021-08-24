# spotadvisor

This package provides an interface for fetching and utilizing [AWS Spot Advisor](https://aws.amazon.com/ec2/spot/instance-advisor/)
data. 

The data is sourced from: https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json

The available data are:

1. Interrupt rate data, described as:
    > Frequency of interruption represents the rate at which Spot has reclaimed 
    capacity during the trailing month. They are in ranges of < 5%, 5-10%, 10-15%, 
    15-20% and >20%.

2. Savings data, described as:
 
    > Savings compared to On-Demand are calculated over the last 30 days. Please 
    note that price history data is averaged across Availability Zones and may be 
    delayed. To view current Spot prices, visit the Spot Price History in the AWS 
    Management Console for up to date pricing information for each Availability 
    Zone.

See [godoc](https://godoc.org/github.com/grailbio/base/cloud/spotfeed) for usage.
