# Block-Based Matrix Multiplication using Hadoop MapReduce & Apache Spark

Here we consider using Hadoop MapReduce and Spark to implement block-based matrix multiplication. 

### Assumptions
  - Each matrix is stored as a text file. 
  - Each line of the file corresponds to a block of the matrix in the format of index-content. 
  - All blocks are 2-by-2 and all matrices are 6-by-6. 
  - Each matrix is divided into 9 blocks of equal size (2-by-2 or 4 elements). 
  - The content of block is stored in a sparse format: (row index, column index, value) where row and column indexes are relative to the block. 
  - All indexes start from 1.
  
  
  
 ### Note
  The first line describes the block A11, 2nd line describes A12. Note also the format as
illustrated (e.g., [] encloses the content of a block; no white spaces used; comma separates the
numbers, etc.).
  
  
  
