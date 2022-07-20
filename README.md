## Info
This is a tool to help query compressed vcf files by the ID column. Originally, the code was written using a combination of Java and SQL which is why the file is named DB.java, but the Tabix tool created by Heng Li is a significantly more memory-efficient querying tool due to its ability to be used on file compressed with bgzip. Due to this, tabix and bgzip are expected to be used jointly with this tool.

## Requirements
- must use "createDB" argument first to be able to query a file
- file being queried must be compressed and tabix indexed
- tabix must be in the PATH
- bgzip must be in the PATH
- must make a list of ids in a text file for the query
- ids must be in a one-per-line format

## Example Usage
\>\>\>java DB createDB targetFile.vcf.gz \
\>\>\>java DB query idList.txt targetFile.vcf.gz
Outputs corresponding id lines if id is found in the field

Example id file format: \
rs1 \
rs2
