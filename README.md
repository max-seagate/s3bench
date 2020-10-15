### Clone the repo.
```
git clone -b data_verification_tool https://github.com/Seagate/s3bench/
```
### Build the code.
```
go build
```
Note: If go is not installed, follow below steps to install "go"
```
    wget https://golang.org/dl/go1.15.2.linux-amd64.tar.gz
    tar -C /usr/local -xzf go1.15.2.linux-amd64.tar.gz
    export PATH=$PATH:/usr/local/go/bin
```
Check go version.
```
go version
```
Expected output : go version go1.15.2 linux/amd64
### To run the wrapper script.
```
./data_verify.sh -K <access_key> -S <access_secret> -b <bucketname> -w <workload_type> -i <no. of iterations> -s <no. of samples> -c <no. of clients>
```
### Workload paramters
```
workload type:
1) write_read_verify
2) overwrite_verify
3) parallel_write_read_verify
4) write_delete_write_verify
K : access key
S : secret key
b : bucketname
w : workload type
i : number of iterations 
s : number of samples (number of objects)
c : number of clients
```
