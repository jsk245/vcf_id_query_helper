Use:
query a compressed vcf file by id

Requirements for use:
must use "createDB" argument first to be able to query a file
file being queried must be compressed and tabix indexed
tabix must be in the PATH
bgzip must be in the PATH
must make a list of ids in a text file for the query
ids must be in a one-per-line format

Output information:
prints information as long as the id is found in the line


Example usage:
1. java DB createDB targetFile.vcf.gz
2. java DB query idList.txt targetFile.vcf.gz
Outputs corresponding id lines if id is found in the field

Example id file format:
rs1
rs2