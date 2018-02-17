This is a simple imterface implementation for UOF 

SImply
Receives feeds from the sdk , extarcts relevant params and pushes to rabbitMQ
for delayed processing

All Event can comfortably be re processed and re-run for as many times as
possible to allow for multiple resend
