## Testing

To validate that everything is processed correctly, I created some
dummy logs to test some of the edge cases.

In total there are:
3 correct fourtwenty messages from Peter Cnus
15 correct leet messages from Peter Anus
and 5 correct zeet messages from Beter Bnus

The files are designed to test 3 aspects:
1. Correctness (obiously)
2. Merging of text logs with different Contact names
3. Majority resolve

To explain 3. a bit more, there can be the case that
mulitple logs do not agree if a message was sent at the same time.
To resolve this problem all logs all considered and only if at least 50% 
of the logs agree on a date only then is the date counted.

i.e. In the logs the message on the 25th of december is appearing once as
writte at the correct time but twice on the incorecct time. Therefore this log
should not be counted. If it's exactly 50:50 it's counted anyway.

 
