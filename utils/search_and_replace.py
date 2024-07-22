import os

print("Directory to Loop Through: ")
basePath = input("> ")

print("Text to find: ")
texToFind = input("> ")

print('Text to us as replacement: ')
replacementText = input(r'> ')

directory = os.fsencode(basePath)

for file in os.listdir(directory):
    fileName = os.fsdecode(file)
    with open(fileName, 'r') as file:
        filedata = file.read()

    filedata = filedata.replace(texToFind, replacementText)

    print(f'Replacing occurences of {texToFind} with {replacementText} in file {fileName}')
    with open(fileName, 'w') as file:
        file.write(filedata)