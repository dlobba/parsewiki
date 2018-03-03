import mwparserfromhell as pfh
import json
import copy

class Token:
    """Class used to label the information just parsed
    and to address further processing."""
    TEXT = 0
    WORD = 1
    ENTITY = 2
    INFOBOX = 3
    WIKICODE = 4 # object must be analysed again
    IGNORE = 5

class Entity:
    """Class describing the entities specified by wikilinks and
    used in the Page class.

    Each entity has a list of positions, to the unstructured part,
    referring to the Entity."""
    
    def __init__(self, name, position=None):
        self.title = name
        if position is None:
            self.positions = []
        else:
            self.positions = [position]

    def to_json(self):
        public_data = {"title": self.title, \
                       "positions": self.positions}
        return json.dumps(public_data)


class Page:
    """Class describing the wikipedia page, the entities
    present in the text. It's made up of two parts: the *structured*
    *part* and the *unstructured* one.

    Note:
    
      The structured part contains the first Infobox found in the
      page.
    """
    
    def __init__(self, title, date=None):
        self.word_count = 0
        self.title = title
        self.date = date
        self.entities = dict()
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

    def add_entity(self, entity_name, word_count=False):
        """Add new entity to the list.

        Args:
          entity_name (str): the name of the entity to be added.
          word_count (bool): add actual counter to entity positions
                             list.
        """
        if not word_count:
            if not entity_name in self.entities:
                self.entities[entity_name] = Entity(entity_name)
        if entity_name in self.entities:
            self.entities[entity_name].positions\
                                      .append(self.word_count)
        else:
            self.entities[entity_name] = Entity(entity_name, \
                                                self.word_count)

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
                    page.unstruct.append((word, \
                                          page.word_count))
                    page.word_count += 1
                #page.unstruct.append((value, page.word_count))
                #page.word_count += 1
            elif info_type == Token.WORD:
                page.unstruct.append((value, \
                                      page.word_count))
                page.word_count += 1
            elif info_type == Token.ENTITY:
                title, text = value
                page.add_entity(title)
                page.unstruct.append((text, page.word_count))
                page.word_count += 1
            elif info_type == Token.INFOBOX:
                if not page.is_struct_freezed():
                    parameters, entities = value
                    for name,value in parameters.items():
                        page.struct[name] = value
                    for entity_name in entities:
                        page.add_entity(entity_name)
                    page.freeze_struct()
            elif info_type == Token.WIKICODE:
                nodes = value.nodes + nodes
        return page

    def to_json(self):
        public_data = {"title": self.title, \
                       "structured_part": self.struct, \
                       "unstructured_part": self.unstruct, \
                       "entities": None}
        public_data["entities"] = [entity.to_json() \
                                   for entity in self.entities.values()]
        return json.dumps(public_data)

# Parse functions:
#   each function allows to establish a fine-grained way
#   to treat each piece of wikicode as individuate by the
#   mwparserfromhell module.

def parse_text(text_node):
    """Separate words in text keeping the spaces,
    if present."""
    return [str(text_node.value)] # remove me
    if text_node.value is None:
        return ""
    return (word + " " for word in text_node.value. \
            strip(). \
            split())

def is_wikilink_entity(wikilink_node):
    """Return true if the current wikilink is a real
    entity (has a corresponding page on Wikipedia)."""
    return not wikilink_node.title\
                            .startswith(( \
                                          "Categoria:", \
                                          "File:"))

def parse_wikilink(wikilink_node):
    """Return wikilink title and visible text.
    If no visible text is avaible, the title is visible, so
    it's returned instead.
    """
    title = wikilink_node.title.split("#")[0]
    if wikilink_node.text:
        text = wikilink_node.text.split("|")[-1]\
               .strip()
        return title, text
    return title, title 

def parse_tag(tag_node):
    """Return anything that is embedded in the tag.

    TODO:
      Add fine-grained control over a limited set of
      tags (br, table...)."""
    return tag_node.contents

def parse_heading(heading_node):
    """Return heading as a unique text."""
    return parse_as_text_chunk(heading_node)

def parse_external_link(extlink_node):
    """External links are visible text, so are treated in the
    same way."""
    extlink_node.value = extlink_node.title
    return parse_text(extlink_node)

def parse_html_entity(html_node):
    """Return unicode html_entity."""
    return html_node.normalize()

def parse_infobox(infobox):
    """Return the set of entities and the parameters found
    in the infobox."""
    entities = set()
    parameters = dict()
    for param in infobox.params:
        text, param_entities = parse_param(param.value)
        param_name = str(param.name)                   # force str type
        parameters[param_name] = text
        entities.update(param_entities)
    return parameters, entities

def parse_template(template_node):
    """Parse templates that are not infoboxes.

    TODO:
      Still to implement.
    """
    pass
    
def parse_as_text_chunk(node):
    """Parse node as a unique chunk of text,
    without splitting it into words.
    """
    return str(node).strip()
                      
def parse_node(current_node, infoboxes=None, handled_template=None):
    """Parse the current node based on his class and
    return the result along with the Token type it belongs
    to.

    This is the controller method for all the parsing.
    Behaviour on how to manage each node parsed is based
    on the Token type returned.
    """
    if type(current_node) == pfh.nodes.text.Text:
        return Token.TEXT, \
            parse_text(current_node)

    elif type(current_node) == pfh.nodes.wikilink.Wikilink:
        is_entity = is_wikilink_entity(current_node)
        if not is_entity:
            return Token.IGNORE, 0
        return Token.ENTITY, parse_wikilink(current_node)

    elif type(current_node) == pfh.nodes.Tag:
        further_data = parse_tag(current_node)
        if further_data is None:
            return Token.IGNORE, 0
        return Token.WIKICODE, further_data

    elif type(current_node) == pfh.nodes.heading.Heading:
        return Token.WORD, parse_as_text_chunk(current_node)

    elif type(current_node) == pfh.nodes.html_entity.HTMLEntity:
        return Token.WORD, parse_html_entity(current_node)
                         
    elif type(current_node) == pfh.nodes.external_link.ExternalLink:
        return Token.TEXT, parse_external_link(current_node)

    elif type(current_node) == pfh.nodes.template.Template:
        # force casting to str (Wikicode otherwise) in
        # order for it to be hashable, therefore allowing
        # to check its presence in the list.
        template_name = str(current_node.name).strip()
        if infoboxes and template_name in infoboxes:
            return Token.INFOBOX, parse_infobox(current_node)
        elif handled_template and current_node.name in handled_template:
            return Token.TEXT, parse_template(current_node)
        else:
            return Token.IGNORE, 0
    else:
        return Token.IGNORE, parse_as_text_chunk(current_node)


def parse_param(source_param):
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
            title, text = value
            entities.add(title)
            words.append(text)
        elif info_type == Token.WIKICODE:
            nodes = value.nodes + nodes
    return str.join("", words), entities
