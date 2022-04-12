import logging
import os
import shutil
import re
from collections import defaultdict
import sys

from bs4 import BeautifulSoup, Tag


def _getSubstring(block, delimiters):
    # No error checking...don't do anything dumb
    return block[delimiters[0]:delimiters[1]]


def _textify(block):
    """
    Smash down any html formatting in the provided string
    """

    # Format html lists as python/Sphinx lists.
    block = block.replace("<li>","<li>* ")

    return re.sub('[^\x00-\x7f]', ' ', BeautifulSoup(block, 'lxml').get_text())  # explicitly scrub non-ascii chars


def _padString(strIn, padding=None):
    """
    Replace every endline with endline + (#padding) spaces, for indent formatting
    """

    if padding is None:
        return strIn
    if not (isinstance(padding, int) and padding >= 0):
        raise ValueError("Invalid padding argument {} ".format(padding))

    pad = ' '*padding
    return pad + strIn.replace('\n', '\n'+pad)


def _docstringify(strIn, padding=None, collapseEmpty=True):
    if strIn is None:
        return None

    stripped = strIn.strip()
    if len(stripped) < 1 and collapseEmpty:
        return None

    if padding is None:
        return stripped
    return _padString('\n' + stripped + '\n', padding)


def _htmlUnescape(htmlIn, parts=None, additionalParts=None):
    if parts is None:
        parts = {
            "&nbsp;": " ", "&#160;": " ", "&#xa0;": " ",  # non-breaking space
            "&#8203;": ""
        }
    if additionalParts is not None:
        parts.update(additionalParts)

    out = htmlIn
    for key in parts:
        out = out.replace(key, parts[key])
    return out


def _findBlock(strIn, startString, endString, startLimit=None, endLimit=None, inclusive=False):
    if startLimit is None:
        startLimit = 0
    if endLimit is None:
        endLimit = len(strIn)
    if endLimit <= startLimit:
        return None

    try:
        start = strIn.index(startString, startLimit, endLimit)
    except ValueError as e:
        return None

    try:
        end = strIn.index(endString, start+len(startString), endLimit)
    except ValueError as e:
        if inclusive:
            return start, None
        else:
            return start+len(startString), None

    if inclusive:
        return start, end+len(endString)
    else:
        return start+len(startString), end

def parseTypeAnchor(anchorElt):
    """
    :param anchorElt: bs4 anchor tag representing a param in a method signature
    :return: string java type, with nested generics
    """
    # each <a> has a title we will use to read the package
    title = anchorElt['title']
    package = title.split()[-1]
    return package + '.' + anchorElt.string

def _parseSignature(sigString, methodName):
    # fix whitespace before parsing, lxml handles that like xml and not like html (consider html parser?)
    dom = BeautifulSoup(_htmlUnescape(sigString), 'lxml')

    # find modifiers span and collect allowed contents
    modifiers = []
    modifiersSpan = dom.find('span', {'class': 'modifiers'})
    if modifiersSpan is not None:
        modifierParts = modifiersSpan.get_text().split(' ')
        # TODO annotations probably aren't here any more
        allowedModifiers = {'public', 'private', 'protected', 'static', 'abstract', 'default', 'final', 'strictfp',
                            'java.lang.@Deprecated', 'io.deephaven.util.annotations.@ScriptApi'}
        for el in modifierParts:
            if el in allowedModifiers:
                modifiers.append(el)
    annotationsSpan = dom.find('span', {'class': 'annotations'})
    if annotationsSpan is not None:
        for child in annotationsSpan.contents:
            if isinstance(child, Tag):
                # two cases, just brute forcing it
                if child.get_text() == "@Deprecated":
                    modifiers.append('java.lang.@Deprecated')
                if child.get_text() == "@ScriptApi":
                    modifiers.append('io.deephaven.util.annotations.@ScriptApi')

    # if generics are specified, collect them as well
    genericsInfo = None
    genericsSpan = dom.find('span', {'class': 'type-parameters'})
    if genericsSpan is not None:
        genericsInfo = ''
        for child in genericsSpan.contents:
            if isinstance(child, str):
                genericsInfo += child.strip('\n')
            else:
                genericsInfo += parseTypeAnchor(child)
        genericsInfo = genericsInfo.strip()

    # return type is always present, iterate children and assemble string
    returnTypeSpan = dom.find('span', {'class': 'return-type'})
    returnType = ''
    for child in returnTypeSpan.contents:
        if isinstance(child, str):
            returnType += child.strip('\n')
        else:
            returnType += parseTypeAnchor(child)
    returnType = returnType.strip()

    paramsSpan = dom.find('span', {'class': 'parameters'})
    currentParamAnnotations = []
    currentParamType = ''
    params = []
    if paramsSpan is not None:
        for child in paramsSpan.contents:
            if isinstance(child, str):
                for line in child.split('\n'):
                    if line.startswith('@'):
                        # annotation on the param
                        currentParamAnnotations.append(line.strip())
                    elif line.endswith(',') or line.endswith(')'):
                        # might be a name, split on whitespace between type and name
                        parts = line.split(' ')
                        if len(parts[0]) > 0:
                            # found more type details, append them too
                            currentParamType += parts[0]
                        if len(parts) > 1:
                            # this is a name, split on space and anything before it is part of the type

                            # extract the type name, minus trailing comma or close paren
                            currentParamName = parts[1][:-1]

                            # finish this param, prep if there is a next one
                            currentParam = [currentParamType, currentParamName]
                            if len(currentParamAnnotations) > 0:
                                currentParam.insert(0, ' '.join(currentParamAnnotations))
                            params.append(currentParam)
                            currentParamType = ''
                            currentParamAnnotations = []
                    else:
                        # plain string, part of the type (primitive, </>/extends/super/etc, append to the last string
                        currentParamType += line.strip()
            if isinstance(child, Tag):
                # anchor, read out the type
                currentParamType += parseTypeAnchor(child)

    return modifiers, genericsInfo, returnType, params

class ClassDocParser(object):
    """This parses the desired components from the provided java doc (page?)"""

    def __init__(self, docString):
        self._docString = docString
        self._methods = defaultdict(self._newMethodItem)
        self._package = None
        self._symbol = None
        self._type = None
        self._text = None

        # parse the symbol information
        self._parseSymbol()
        # parse the method details
        self._parseMethods()
        # todo: parse any other details?

    @property
    def docString(self):
        """The provided doc string"""

        return self._docString

    @property
    def methods(self):
        """Dictionary of the form {'<symbol>#method' : MethodDetails object}"""

        return self._methods

    @property
    def className(self):
        """The class name for this class"""
        if self._package is None or self._symbol is None:
            raise ValueError("Package or Symbol not parsed successfully")
        return self._package + '.' + self._symbol

    @property
    def pathName(self):
        """The fully qualified path name for this class"""

        if self._package is None or self._symbol is None:
            raise ValueError("Package or Symbol not parsed successfully")

        return self.className.replace('$', '.')

    @property
    def isNested(self):
        """Is this nested in another class?"""

        if self._symbol is None:
            raise ValueError("Symbol not parsed successfully")
        return '$' in self._symbol

    @property
    def parentPath(self):
        """The parent path if nested class/interface, or None"""

        if not self.isNested:
            return None
        ind = self._symbol[-1::-1].index('$')
        return self._package + '.' + self._symbol[:-ind]

    @property
    def type(self):
        """interface, enum, or class?"""

        return self._type

    @property
    def text(self):
        """Document string for the class itself"""

        return self._text

    def __str__(self):
        return 'ClassDocParser<< pathName={}, type={} >>'.format(self.pathName, self.type)

    def __repr__(self):
        out = []
        for key in sorted(self.methods.keys()):
            out2 = ',\n'.join([str(meth) for meth in self.methods[key]])
            out.append('{}=[\n{}\n]'.format(key, out2))
        if self.isNested:
            return 'ClassDocParser(\n' + \
                   'pathName={}\n,'.format(self.pathName) + \
                   'className={}\n,'.format(self.className) + \
                   'methods={\n' + ',\n'.join(out) + '})'
        else:
            return 'ClassDocParser(\n' + \
                   'pathName={}\n,'.format(self.pathName) + \
                   'methods={\n' + ',\n'.join(out) + '})'

    @staticmethod
    def _newMethodItem():
        """Helper method"""

        return []

    def _parseSymbol(self):
        # find the symbol information
        classStartBlock = '<!-- ======== START OF CLASS DATA ======== -->'

        packageStartBlock = '<a href="package-summary.html">'
        packageEndBlock = '</a'

        symbolStartBlock = '<h1'
        symbolEndBlock = '</h1>'

        symbolInfoDelimiters = _findBlock(self.docString, classStartBlock, symbolEndBlock, inclusive=True)
        if symbolInfoDelimiters is None:
            raise ValueError('Failed to find the symbol information block')
        symbolInfoBlock = _getSubstring(self.docString, symbolInfoDelimiters)

        packageInfoDelimiters = _findBlock(symbolInfoBlock, packageStartBlock, packageEndBlock, inclusive=True)
        if packageInfoDelimiters is None:
            raise ValueError('Failed to find the package block inside the symbol '
                             'information block = {}'.format(symbolInfoBlock))
        pack = _textify(_getSubstring(symbolInfoBlock, packageInfoDelimiters)).strip().split()[-1]
        self._package = pack

        symbolBlockDelimiters = _findBlock(symbolInfoBlock, symbolStartBlock, symbolEndBlock, inclusive=True)
        if symbolBlockDelimiters is None:
            raise ValueError('Failed to find the symbol block inside the symbol '
                             'information block = {}'.format(symbolInfoBlock))
        symb = _textify(_getSubstring(symbolInfoBlock, symbolBlockDelimiters)).strip()
        # is this a class or an interface?
        temp = symb.lower().split()
        if 'interface' in temp:
            self._type = 'interface'
        elif 'enum' in temp:
            self._type = 'enum'
        else:
            self._type = 'class'
        # get rid of bracket crapola
        try:
            ind = symb.index('<')
            symb = symb[:ind]
        except ValueError:
            pass
        # get rid of any initial cruft
        symb = symb.split()[-1]
        symb = symb.replace('.', '$')
        self._symbol = symb

        # Try to parse the text for this class/enum/interface
        classDetailsStartBlock = '<section class="description">'  # after symbolEndBlock
        classDetailsEndBlock = '<section class="summary">'
        classSpecificStart = '<pre>'
        classSpecificEnd = '</pre>'
        textStart = '<div class="block">'  # directly after class specific stuff
        textEnd = "</div>"
        classDetailsDelimiters = _findBlock(self.docString, classDetailsStartBlock, classDetailsEndBlock,
                                            startLimit=symbolInfoDelimiters[1], inclusive=False)
        if classDetailsDelimiters is not None:
            classBlock = _getSubstring(self.docString, classDetailsDelimiters)
            # find the class specific stuff
            classSpecificDelimiters = _findBlock(classBlock, classSpecificStart, classSpecificEnd, inclusive=True)
            if classDetailsDelimiters is not None:
                textDelimiters = _findBlock(classBlock, textStart, textEnd,
                                            startLimit=classSpecificDelimiters[1], inclusive=True)
                if textDelimiters is not None:
                    self._text = _textify(_getSubstring(classBlock, textDelimiters))

    def _parseMethods(self):
        # look for a methods section
        methodStartString = '<h2>Method Details</h2>'
        methodEndString = '</section>\n</li>\n</ul>\n</section>'
        limits = _findBlock(self.docString, methodStartString, methodEndString, inclusive=False)
        if limits is not None:
            methodBlockString = self.docString[limits[0]:limits[1]]
            thisStart = 0
            theEnd = len(methodBlockString)
            # iterate over each method and populate
            while (thisStart is not None) and thisStart < theEnd:
                methodLimits = _findBlock(methodBlockString, '<li>\n<section class="detail"',
                                          '</section>\n</li>', thisStart, theEnd, inclusive=True)
                if methodLimits is not None:
                    if self.type == 'interface':
                        defMods = {'public', }  # everything for an interface is implicitly public
                    else:
                        defMods = set()
                    methodDetail = MethodDetail(methodBlockString, methodLimits[0], methodLimits[1], defaultModifiers=defMods)
                    self.methods[methodDetail.name].append(methodDetail)
                    thisStart = methodLimits[1]
                else:
                    thisStart = None


class MethodDetail(object):
    ignoreInSignature = {','}

    def __init__(self, strIn, start, end, defaultModifiers=set()):
        self.name = None
        self.modifiers = None
        self.genericsInfo = None
        self.returnType = None
        self.returnText = None
        self.parameters = []
        self.parameterTypes = []
        self.parametersText = {}
        self.text = None

        self.documentBlock = strIn[start:end]

        thisEnd = len(self.documentBlock)
        step = self._getName(0, thisEnd)
        if step is not None:
            step = self._getSignature(step, thisEnd)
        # add in any default modifiers
        if self.modifiers is None:
            self.modifiers = defaultModifiers
        else:
            self.modifiers = defaultModifiers.union(self.modifiers)
        # make parameters & parameters a tuple - must be hashable
        self.parameters = tuple(self.parameters)
        self.parameterTypes = tuple(self.parameterTypes)

        if step is not None:
            step = self._getText(step, thisEnd)
        if step is not None:
            step = self._getParameterDetails(step, thisEnd)

    def __str__(self):
        out = []
        for arg in ['name', 'modifiers', 'genericsInfo', 'text', 'parameters', 'parameterTypes',
                    'parametersText', 'returnType', 'returnText']:
            val = getattr(self, arg)
            if val is not None:
                out.append('{}={}'.format(arg, val))
        return 'MethodDetail(\n\t' + ',\n\t'.join(out) + ')'

    def _getName(self, start, end):
        """Parses name and returns the end of the name block"""

        nameStartString = '<h3'
        nameEndString = '</h3>'

        nameDelimiters = _findBlock(self.documentBlock, nameStartString, nameEndString, start, end, inclusive=False)
        if nameDelimiters is not None:
            if nameDelimiters[1] is not None:
                self.name = self.documentBlock[nameDelimiters[0]:nameDelimiters[1]].split('>')[1]
                return nameDelimiters[1] + len(nameEndString)
            else:
                self.name = self.documentBlock[nameDelimiters[0]:end].split('>')[1]
        return None

    def _getSignature(self, start, end):
        """Parses signature and returns the end of the signature block"""
        sigStartString = ['<div class="member-signature">', '<pre>']
        sigEndString = '</div>'
        sigDelimiters = None
        for sigStartStr in sigStartString:
            if sigDelimiters is None:
                sigDelimiters = _findBlock(self.documentBlock, sigStartStr, sigEndString,
                                           start, end, inclusive=False)
        if sigDelimiters is None or sigDelimiters[1] is None:
            return None

        modifiers, genericsInfo, returnType, params = _parseSignature(self.documentBlock[sigDelimiters[0]: sigDelimiters[1]], self.name)
        self.modifiers = modifiers
        self.genericsInfo = genericsInfo
        self.returnType = returnType

        badParsing = False
        for seg in params:
            el = [entry for entry in seg if entry not in self.ignoreInSignature]
            if len(el) == 2:
                self.parameterTypes.append(el[0])
                self.parameters.append(el[1])
            elif len(el) == 3:
                self.parameterTypes.append(el[1])
                self.parameters.append(el[2])
            else:
                logging.error("Misparsed argument {}".format(el))
                badParsing = True
        if badParsing:
            logging.error('Evidently bad parsing for the parameters in {}'.format(
                _htmlUnescape(self.documentBlock[sigDelimiters[0]: sigDelimiters[1]])))
            raise ValueError

        return sigDelimiters[1] + len(sigEndString)

    def _getText(self, start, end):
        """Parses method text - if it's there - and returns the next starting point"""

        textStartString = '<div class="block">'
        textEndString = '</div>'
        block = None

        while block is None:
            textDelimiters = _findBlock(self.documentBlock, textStartString, textEndString, start, end, inclusive=False)
            if textDelimiters is None or textDelimiters[1] is None:
                return start

            block = self.documentBlock[textDelimiters[0]:textDelimiters[1]]
            # we should squish the html formatting out of the text

            if "Description copied" in block:
                block = None
                start = textDelimiters[1]

        self.text = _textify(block)
        return textDelimiters[1] + len(textEndString)

    def _getParameterDetails(self, start, end):
        """Parses parameter details text - if it's there - and returns the next starting point"""

        paramStartString = '<dt>Parameters:</dt>\n'
        returnStartString = '<dt>Returns:</dt>\n'
        blockEnd = '</dl>'

        paramsDelimiters = _findBlock(self.documentBlock, paramStartString, blockEnd, start, end, inclusive=False)
        returnsDelimiters = _findBlock(self.documentBlock, returnStartString, blockEnd, start, end, inclusive=False)

        paramsBlock = None
        returnsBlock = None
        endPoint = start
        if paramsDelimiters is None and returnsDelimiters is None:
            return start
        elif returnsDelimiters is None:
            # just params block
            paramsBlock = self.documentBlock[paramsDelimiters[0]:paramsDelimiters[1]]
            endPoint = paramsDelimiters[1] + len(blockEnd)
        elif paramsDelimiters is None:
            # just returns block
            returnsBlock = self.documentBlock[returnsDelimiters[0]:returnsDelimiters[1]]
            endPoint = returnsDelimiters[1] + len(blockEnd)
        else:
            # both are present
            paramsBlock = self.documentBlock[paramsDelimiters[0]: returnsDelimiters[0]-len(returnStartString)]
            returnsBlock = self.documentBlock[returnsDelimiters[0]:returnsDelimiters[1]]
            endPoint = returnsDelimiters[1] + len(blockEnd)

        entryStartString = '<dd>'
        entryEndString = '</dd>'
        pNameStartString = '<code>'
        pNameEndString = '</code>'

        if returnsBlock is not None:
            returnTextDelimiters = _findBlock(returnsBlock, entryStartString, entryEndString, inclusive=False)
            if returnTextDelimiters is not None:
                self.returnText = _textify(returnsBlock[returnTextDelimiters[0]:returnTextDelimiters[1]])

        if paramsBlock is not None:
            paramsStep = 0
            while (paramsStep is not None) and (paramsStep < len(paramsBlock)):
                thisParamDelimiters = _findBlock(paramsBlock, entryStartString, entryEndString, paramsStep, inclusive=False)
                paramsStep = None
                if thisParamDelimiters is not None:
                    paramsStep = thisParamDelimiters[0]
                    paramNameDelimiters = _findBlock(paramsBlock, pNameStartString, pNameEndString, paramsStep, inclusive=False)
                    paramsStep = None
                    if paramNameDelimiters is not None:
                        self.parametersText[paramsBlock[paramNameDelimiters[0]:paramNameDelimiters[1]]] = \
                            _textify(paramsBlock[paramNameDelimiters[1] + 7:thisParamDelimiters[1]])
                        paramsStep = thisParamDelimiters[1] + len(entryEndString)
        return endPoint

    def createDocString(self, padding=None, excludeText=False, collapseEmpty=True):
        out = ""
        if (self.text is not None) and (len(self.text) > 0) and (not excludeText):
            out += '{}\n\n'.format(self.text)
        if self.genericsInfo is not None:
            out += 'Note: Java generics information - {}\n\n'.format(self.genericsInfo)
        for pname, ptype in zip(self.parameters, self.parameterTypes):
            pText = self.parametersText.get(pname, None)
            if pText is None:
                out += ':param {}: {}\n'.format(pname, ptype)
            else:
                out += ':param {}: ({}) {}\n'.format(pname, ptype, pText)

        if self.returnType is not None and self.returnType != 'void':

            if self.returnText is None:
                out += ':return: {}\n'.format(self.returnType)
            else:
                out += ':return: ({}) {}\n'.format(self.returnType, self.returnText)
        return _docstringify(out, padding, collapseEmpty=collapseEmpty)


def methodDigest(methodDetailList, details, requiredModifiers={'public'}, maxCount=5, padding=None, verbose=False):
    maxMaxCount = 50

    try:
        maxCount = int(maxCount)
    except ValueError:
        maxCount = 5
    finally:
        if maxCount < 1:
            logging.warning("maxCount was set to {} (< 1), and will be redefined as 1".format(maxCount))
            maxCount = 1
        if maxCount > maxMaxCount:
            logging.warning("maxCount was set to {} (> {}), and will be redefined as {}".format(maxCount, maxMaxCount, maxMaxCount))
            maxCount = maxMaxCount

    useList = []
    for el in methodDetailList:
        mods = requiredModifiers.intersection(el.modifiers)
        if mods == requiredModifiers:
            useList.append(el)
    if len(useList) < 1:
        return _docstringify(None, padding)

    # Is there just one MethodDetail? If so, just return a decent doc string
    if len(useList) == 1:
        return useList[0].createDocString(padding)

    # digest all the things
    text = set()
    for el in useList:
        if el.text is None:
            text.add("")
        else:
            text.add(el.text.strip())
    # Is there just one text?
    if len(text) == 1:
        textPart = text.pop()
    else:
        texts = {el.text for el in useList if el.text is not None}
        texts = list(texts)
        texts.sort()

        if len(texts) == 0:
            textPart = None
        elif len(texts) == 1:
            textPart = texts[0]
        else:
            textPart = "**Incompatible overloads text - text from the first overload:**\n\n{}".format(texts[0])

            if verbose:
                className = details["className"]
                print(f"vvvvv INCOMPATIBLE JAVADOC FOR PYTHON vvvvv")
                print(f"\tclassName: {className}\n")
                print(f"\t{useList[0]}\n")
                for i in range(len(texts)):
                    txt = texts[i].replace("\n"," ")
                    print(f"\tdocstring {i}: {txt}")
                print(f"^^^^^ INCOMPATIBLE JAVADOC FOR PYTHON ^^^^^")

    if textPart is None:
        out = ""
    else:
        out = '{}\n\n'.format(textPart.strip())
    if len(useList) > 2*maxCount-1:
        out += "There are {} overloads, restricting signature summary to first {}:\n".format(len(useList), maxCount)
        for i, md in enumerate(useList[:maxCount]):
            out += "*Overload {}*{}\n".format(i+1, md.createDocString(padding=2, excludeText=True, collapseEmpty=False))
    else:
        for i, md in enumerate(useList):
            out += "*Overload {}*{}\n".format(i+1, md.createDocString(padding=2, excludeText=True, collapseEmpty=False))
    return _docstringify(out, padding)


if __name__ == '__main__':
    # NOTE: this will fail (currently) unless the working directory is this location
    from docGenUtil import populateCurrentDocs, classDocGeneration, finalize

    maxSignatures = 50
    verbose = False

    # NOTE: weak arg parsing here, do we need more?
    if len(sys.argv) < 2:
        raise ValueError("The script requires at least one argument: devroot")

    if sys.argv[1].lower() in ['-h', '--help']:
        print("Called as:\n"
              "  python javadocExtraction.py <devroot> <assertNoChange>[False]\n"
              "\n"
              "    - <devroot> specifies the development root, below which we expect directories\n"
              "        `build/docs/javadoc` and `Integrations/python/deephaven/doc`\n"
              "    - <assertNoChange> [default `False`] optional argument.\n"
              "        * False indicates to extract the javadocs to .json format below\n"
              "           `Integrations/python/deephaven/doc`\n"
              "        * True indicates to check that the .json files in the file system below\n"
              "           `Integrations/python/deephaven/doc` match what WOULD be generated.\n"
              "           **NO ACTUAL GENERATION HERE**")

    # Parse the arguments
    devRoot = sys.argv[1]

    assertNoChange = False
    if len(sys.argv) > 2:
        assert_t = sys.argv[2].lower()
        if assert_t in ['true', 't', '1']:
            assertNoChange = True

    docRoot = os.path.join(devRoot, 'javadoc')
    outDir = os.path.join(devRoot, 'out', 'doc')

    # junk any contents of outDir, if it exists - it's easier than trying to sync somehow
    if (not assertNoChange) and os.path.exists(outDir):
        shutil.rmtree(outDir)

    # walk the contents of outDir, and figure the current list of javadoc extracts
    currentDocs = populateCurrentDocs(outDir)

    # walk down the com directory of docRoot, and find all the html files
    for root, dirs, files in os.walk(os.path.join(docRoot, 'io')):
        for fil in files:
            fstem, fext = os.path.splitext(fil)
            if (fstem[0] == '.') or (fext != '.html') or (fstem.startswith('package-')):
                continue

            # parse the file
            with open(os.path.join(root, fil), 'r', encoding="utf8") as fi:
                classDetails = ClassDocParser(fi.read())
            logging.info('Converting docs for {}'.format(classDetails))
            # get classname, pathname and text for class/interface/enum itself
            className = classDetails.className
            pathName = classDetails.pathName
            symbDocString = _docstringify(classDetails.text, padding=None)

            # prepare the docstring dictionary
            details = {"className": className, "path": pathName, "typeName": classDetails.type}
            if symbDocString is None:
                logging.info("className = {} has empty doc string".format(className))
            else:
                details["text"] = symbDocString

            # parse details for explicit methods
            methodDetails = {}
            for methodName in classDetails.methods:
                methodList = classDetails.methods[methodName]
                entryDocString = methodDigest(methodList, details, requiredModifiers={'public'}, maxCount=maxSignatures, padding=None, verbose=verbose)
                if entryDocString is None:
                    logging.info("className = {}, methodName = {} has empty docstring".format(className, methodName))
                else:
                    methodDetails[methodName] = entryDocString
            details["methods"] = methodDetails

            # finalize the generation task for this class
            classDocGeneration(currentDocs, assertNoChange, details, outDir)

    finalize(currentDocs, assertNoChange, '\nTo resolve failure, run the task "./gradlew :Generators:generatePyDoc" '
                                          'to regenerate, and then commit the generated changes.\n'
                                          'To diagnose trouble, run the generation task followed by \"git diff\" to see the changes.\n'
                                          'To diagnose possible indeterminism in the generation process, regenerate the code and check '
                                          'the diff **multiple times**.')
