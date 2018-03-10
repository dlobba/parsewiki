import mwparserfromhell as pfh
import json
import copy

from re import split as re_split
from re import sub as re_replace

class Token:
    """Class used to label the information just parsed
    and to address further processing."""
    TEXT = 0
    WORD = 1
    ENTITY = 2
    INFOBOX = 3
    WIKICODE = 4 # object must be analysed again
    IGNORE = 5

class Page:
    """Class describing the wikipedia page.
    It's made up of two parts: the *structured*
    *part* and the *unstructured* one.

    Note:
      The structured part contains the first Infobox found in the
      page.
    """
    
    def __init__(self, title, timestamp=None):
        self.word_count = 0
        self.title = title
        self.timestamp = timestamp
        self.struct = dict()
        
        # once the struct_part is freezed it's no more
        # possible to add new items to it.
        # This behaviour is used to add elements belonging
        # only to the first infobox found.
        self._freeze_struct = False
        self.unstruct = []


    def is_struct_freezed(self):
        """Return freeze status on the structured part."""
        return self._freeze_struct
    
    def freeze_struct(self):
        """Freeze the structured part."""
        self._freeze_struct = True
        
    def unfreeze_struct(self):
        """Unfreeze the structured part."""
        self._freeze_struct = False

    @classmethod
    def parse_page(cls, source_page, title=None, date=None, infoboxes=None):
        """Given a page Wikicode return the information
        encoded within a Page object.

        Args:
          title (str): the page (entity) title.
          date (str): the date of its last revision.
          infoboxes: the set of templates representing infoboxes.

        Returns:
          A Page object.

        Note:
          `infoboxes` is needed only for non wikipage that
          do not follow an explicit convention to represent
          them (like for the Italian Wikipedia where there is
          no way to distinguish between infobox templates
          and other kind of templates.
        """
        page = Page(title, date)
        # make a shallow copy (why source_page.nodes[:]
        # doesn't work?)
        nodes = copy.copy(source_page.nodes)
        while nodes:
            node = nodes.pop(0)
            info_type, value = parse_node(node, infoboxes)
            if info_type == Token.TEXT:
                for word in value:
                    page.word_count += 1
                    page.unstruct.append((word, \
                                          None, \
                                          page.word_count))
            elif info_type == Token.WORD:
                page.word_count += 1
                page.unstruct.append((value, \
                                      None, \
                                      page.word_count))
            elif info_type == Token.ENTITY:
                text, url = value
                page.word_count += 1
                page.unstruct.append((text, \
                                      url, \
                                      page.word_count))
            elif info_type == Token.INFOBOX:
                if not page.is_struct_freezed():
                    parameters, entities = value
                    for name,value in parameters.items():
                        if not entities:
                            page.struct[name] = (value, None)
                        else:
                            for entity in entities:
                                page.struct[name] = (value, entity)
                    page.freeze_struct()
            elif info_type == Token.WIKICODE:
                nodes = value.nodes + nodes
        return page

    def to_json(self):
        public_data = {"title": self.title, \
                       "timestamp": self.timestamp, \
                       "structured_part": None, \
                       "unstructured_part": self.unstruct}

        public_data["structured_part"] = [(name, text, entity) \
                                          for name, (text, entity) \
                                          in self.struct.items()]
        # TODO: be sure to check that conversion is done
        # as expected.
        return json.dumps(public_data)

# Parse functions:
#   each function allows to establish a fine-grained way
#   to treat each piece of wikicode as individuated by the
#   mwparserfromhell module.

def parse_text(text_node):
    """Separate words in text without keeping spaces."""
    if text_node.value is None:
        return None
    words = re_split("[^\w]", text_node.value.strip())
    # clean empty strings
    words = [word for word in words if len(word) > 0]
    return words

def is_wikilink_entity(wikilink_node):
    """Return true if the current wikilink is a real
    entity (has a corresponding page on Wikipedia).
    
    This avoids to link to files, images or videos."""
    media_namespaces = [ \
                         "File:", \
                         "Media:", \
                         "Audio:", \
                         "Video:"]
    media_namespaces.extend([":" + name for name in media_namespaces])
    media_namespaces = tuple(media_namespaces)
    return not wikilink_node.title\
                            .startswith(media_namespaces)

def get_wikilink_url(wikilink_title, lang="en"):
    """
    This is not used and is kept only for further mind boggling.
    In this way, we do not know how to obtain url, and another
    program will have to do this work.
    """
    def get_language(wiki):
        # if this holds then either we have a language link
        # since special links (like :Category:) cannot are individuated
        # by is_wiki_enitity
        if first_colon == 0:
            second_colon = wikilink_title.find(":", 1)
            lang = wikilink_title[first_colon + 1 : second_colon] # here we suppose is a language
        return lang
            
    wiki_language_projects = ("wikipedia", \
                            "wiktionary", "wikt", \
                            "wikinews", "wikibooks", \
                            "wikiquote", "wikisource", \
                            "wikiversity", "wikivoyage")
    wiki_lang_alternative = {"w": "wikipedia", \
                             "simple": "wikipedia", \
                             "n": "wikinews", \
                             "b": "wikibooks", \
                             "q": "wikiquote", \
                             "s": "wikisource", \
                             "v": "wikiversity", \
                             "voy": "wikivoyage"}
    wiki_static_projects = {\
                            "wikispecies": "species.wikimedia.org/wiki/", \
                            "species": "species.wikimedia.org/wiki/", \
                            "oldwikisource": "wikisource.org/wiki/", \
                            "wikidata": "wikidata.org/wiki/", \
                            "d": "wikidata.org/wiki/", \
                            "wikimedia": "wikimediafoundation.org/wiki/", \
                            "foundation": "wikimediafoundation.org/wiki/", \
                            "wmf": "wikimediafoundation.org/wiki/", \
                            "commons": "commons.wikipedia.org/wiki/", \
                            "c": "commons.wikipedia.org/wiki/", \
                            "metawikipedia": "meta.wikipedia.org/wiki/", \
                            "meta": "meta.wikipedia.org/wiki/", \
                            "m": "meta.wikipedia.org/wiki/"}

    first_colon = wikilink_title.find(":")
    if first_colon < 0:
        return lang + "." + "wikipedia.org/wiki/"
    
def parse_wikilink(wikilink_node):
    """Return wikilink title and visible text.
    If no visible text is avaible, the title is visible, so
    it's returned instead.
    """
    title = wikilink_node.title.split("#")[0]
    # perform Wikipedia URL encoding
    # TODO: maneage here url
    # as of now, without further decisions, url is the same
    # as title.
    url = title

    # if text exists and is not empty
    if wikilink_node.text and \
       len(wikilink_node.text.strip()) > 0:
        text = wikilink_node.text.split("|")[-1]\
               .strip()
        return text, url
    return title, url

def parse_tag(tag_node):
    """Return anything that is embedded in the tag.

    TODO:
      Add fine-grained control over a limited set of
      tags (br, table...)."""
    return tag_node.contents

def parse_heading(heading_node):
    """Recursively parse heading content."""
    return heading_node.title
    
def parse_external_link(extlink_node):
    """Return title and url of the external link."""
    url = str(extlink_node.url).strip()
    if not extlink_node.title:
        title = url
    else:
        title = parse_text_only(extlink_node.title)
    return title, url

def parse_html_entity(html_node):
    """Return unicode html_entity."""
    return html_node.normalize()

def is_infobox(template_name):
    """Return true if the given template refers
    to an infobox."""
    return template_name.startswith("Infobox")
    
def parse_infobox(infobox):
    """Return the set of entities and the parameters found
    in the infobox."""
    entities = set()
    parameters = dict()
    for param in infobox.params:
        text = parse_param(param.value)
        param_name = str(param.name)         # force str type
        parameters[param_name] = text
    return parameters

def parse_magic_word(template):
    """Parse a limited subset of magic words, mainly
    concerned with text formatting.

    Returns:
      The plaintext the template is trying to format.

    Notes:
      The magic words managed are:
      
      * lc, lcfirst
      * uc, ucfirst
      * formatnum
      * #dateformat
      * #formatdate"""
    magic_words = ("lc", "lcfirst", "uc", "ucfirst", \
                "formatnum", "#dateformat", "#formatdate")
    magic_word = template.name.split(":")
    value_exists = len(magic_word) > 1
    name = magic_word[0].strip()
    if value_exists and \
       name in magic_words:
        value = magic_word[1].strip()
        return value
    return None
                
def parse_template(template_node):
    """Parse templates that are not infoboxes.

    As of now, this will try to parse magic_words
    related to text `formatting` only, since these
    are of more interest.
    """
    return parse_magic_word(template_node)
    
def parse_as_text_chunk(node):
    """Parse node as a unique chunk of text,
    without splitting it into words.

    Note:
      Mainly used for debug.
    """
    return str(node).strip()
                      
def parse_node(current_node, infoboxes=None):
    """Parse the current node based on his class and
    return the result along with the Token type it belongs
    to.

    This is the controller method for all the parsing.
    Behaviour on how to manage each node parsed is based
    on the Token type returned.
    """
    if type(current_node) == pfh.nodes.text.Text:
        node_text = parse_text(current_node)
        if len(node_text) > 0:
            return Token.TEXT, \
                node_text
        else:
            return Token.IGNORE, \
                None
    elif type(current_node) == pfh.nodes.wikilink.Wikilink:
        is_entity = is_wikilink_entity(current_node)
        if not is_entity:
            return Token.IGNORE, 0
        return Token.ENTITY, parse_wikilink(current_node)

    elif type(current_node) == pfh.nodes.Tag:
        further_data = parse_tag(current_node)
        if further_data is None:
            return Token.IGNORE, None
        return Token.WIKICODE, further_data

    elif type(current_node) == pfh.nodes.heading.Heading:
        return Token.WIKICODE, parse_heading(current_node)

    elif type(current_node) == pfh.nodes.html_entity.HTMLEntity:
        return Token.WORD, parse_html_entity(current_node)
                         
    elif type(current_node) == pfh.nodes.external_link.ExternalLink:
        return Token.ENTITY, parse_external_link(current_node)

    elif type(current_node) == pfh.nodes.template.Template:
        # force casting to str (Wikicode otherwise) in
        # order for it to be hashable, therefore allowing
        # to check its presence in the list.
        template_name = str(current_node.name).strip()
        if is_infobox(template_name):
            return Token.INFOBOX, parse_infobox(current_node)
        else: 
            node_text = parse_template(current_node)
            if node_text:
                return Token.TEXT, node_text
            else:
                return Token.IGNORE, None
    else:
        return Token.IGNORE, parse_as_text_chunk(current_node)


def parse_param(source_param, infoboxes=None):
    """Parse infobox parameters' wikicode.

    Each wikicode is parsed in the same fashion as for the page
    wikicode.

    Args:
      source_param (Wikicode): the wikicode contained in as parameter
                               value.

    Notes:
      It's the equivalent of parse page for wikicode present
      in parameters' value.
    """
    entities = set()
    words = []
    nodes = copy.copy(source_param.nodes)
    while nodes:
        node = nodes.pop(0)
        info_type, value = parse_node(node, infoboxes)
        if info_type == Token.TEXT:
            words.extend(list(value))
        if info_type == Token.WORD:
           words.append(value)
        elif info_type == Token.ENTITY:
            text, url = value
            entities.add(url)
            words.append(text)
        elif info_type == Token.WIKICODE:
            nodes = value.nodes + nodes
    return str.join("", words), entities

def parse_unstruct_wikicode(source_wikicode, infoboxes=None):
    """Parse nested wikicode, belonging to the unstructured
    part.

    Each wikicode is parsed in the same fashion as for the page
    wikicode.

    Args:
      source_wikicode (Wikicode): the wikicode contained in as parameter
      value.

    Notes:
    
      Infoboxes are **ignored**.

      It's the equivalent of parse page for wikicode present
      in nested nodes' values.
    """
    nested_part = []
    nodes = copy.copy(source_wikicode.nodes)
    while nodes:
        node = nodes.pop(0)
        info_type, value = parse_node(node, infoboxes)
        if info_type == Token.TEXT:
            nested_part.extend([(word, None) for word in value])
        if info_type == Token.WORD:
            nested_part.extend((value, None))
        elif info_type == Token.ENTITY:
            text, url = value
            nested_part.extend((text, url))
        elif info_type == Token.WIKICODE:
            nodes = value.nodes + nodes
    return nested_part

def parse_text_only(source_wikicode):
    parsed_wikicode = parse_unstruct_wikicode(source_wikicode)
    if not len(parsed_wikicode) > 0:
        return ""
    only_words = [word for word,_ in parsed_wikicode]
    return str.join(" ", only_words)
