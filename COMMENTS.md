# Setup

## Notes
  * Tested against Node v10.15.3
  * Answers are output to stdout via console.log as README never specified output
  * Header's record count considered source of truth and extra record not included (72 in txnlog.dat, but 71 according to header)
  * All supporting parsing code would be moved into a separate library in production code, exposing the bufferReader objects per type.

## Installation
`npm i`

There are two dependencies `esm` for allowing modules in node and `bigint-buffer` for dealing with BigInts from a buffer

## Run
`npm start`