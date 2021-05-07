import os
import json
import logging

"""
Utility steps for generation of docstring snippets into .json files - for use during Python code generation 
"""


# json creation variables
# NOTE: these values are important for the assertNoChange=True behavior
_sort_keys = True
_indent = 1


def checkDir(theDir):
    if not os.path.exists(theDir):
        os.makedirs(theDir)


def populateCurrentDocs(outDir):
    currentList = {}
    if os.path.isdir(outDir):
        for root, dirs, files in os.walk(outDir):
            for fil in files:
                if fil[0] != '.' and os.path.splitext(fil)[1] == '.json':
                    wholeFile = os.path.join(root, fil)
                    pathName = wholeFile[len(outDir)+1:-5].replace('/', '.')
                    currentList[pathName] = {'file': wholeFile, 'state': 0}
    return currentList


def classDocGeneration(currentDocs, assertNoChange, details, outDir):
    """
    Utility for checking the final generation step

    :param currentDocs: populated state dictionary for current location
    :param assertNoChange: boolean designating whether to check present and new data for for sameness,
           or to generate new json
    :param details: the dictionary that we are going to write as .json
    :param outFile: the location of the .json file
    :return: Nothing. This modifies the currentDocs dictionary in place
    """

    # in this case, we are going to have currentDocs[pathName]['state']:
    #   * 0 - if the file currently exists, but would not get recreated
    #   * 1 - if the two files MATCH - i.e. this is normal
    #   * 2 - if the two files do not match
    #   * 3 - if the file does not currently exists, but would be created

    pathName = details['path']
    # create the output filename
    outFile = os.path.join(outDir, *pathName.split('.')) + '.json'

    # prepare the output directory
    checkDir(os.path.split(outFile)[0])
    newCode = json.dumps(details, sort_keys=_sort_keys, indent=_indent, ensure_ascii=False).strip()

    if assertNoChange:
        if not os.path.exists(outFile):
            currentDocs[pathName] = {'file': outFile, 'state': 3}
        else:
            with open(outFile, 'r', encoding='utf8') as fi:
                oldCode = fi.read().strip()
            if newCode == oldCode:
                currentDocs[pathName]['state'] = 1
            else:
                currentDocs[pathName]['state'] = 2
    else:
        # write the json file - we don't really care about currentDocs, but will update it
        logging.info("Creating {}".format(outFile))
        currentDocs[pathName] = {'file': outFile, 'state': 1}
        with open(outFile, 'w', encoding='utf8') as fi:
            fi.write(newCode)


def finalize(currentDocs, assertNoChange, finalMessage):
    if not assertNoChange:
        return
    # check the contents of currentDocs for badness
    noLongerExist = []
    failToMatch = []
    toBeCreated = []
    for pathName in currentDocs:
        val = currentDocs[pathName]
        if val['state'] == 0:
            noLongerExist.append(pathName)
        elif val['state'] == 2:
            failToMatch.append(pathName)
        elif val['state'] == 3:
            toBeCreated.append(pathName)
    msg = ""
    if len(noLongerExist) > 0:
        msg += "-Documentation extracts to be removed for {}-\n".format(noLongerExist)
    if len(failToMatch) > 0:
        msg += "-Documentation extracts from {} changed-\n".format(failToMatch)
    if len(toBeCreated) > 0:
        msg += "-Documentation extracts to be created for {}-\n".format(toBeCreated)
    if len(msg) > 0:
        raise ValueError(msg + finalMessage)
