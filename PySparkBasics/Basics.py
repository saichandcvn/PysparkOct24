a = 1

print("Using while loop:")

while(a<=10):
    print(a)
    a = a+1

print("Using for loop with list:")

mylist = [1,2,3,4,5]

for i in mylist:
    print(i)

print("Using Tuple:")

mytup=(1,2,3,"apple")
print(mytup[0])
print(mytup[3])

print("Using Dictionary:")

mydict={"name":"sai","age":"32","city":"hyd"}
print(mydict["city"])

print("Using Dictionary Keys:")

for key in mydict:
    print(key)

print("Using Dictionary Values:")

for value in mydict:
    print(value)
