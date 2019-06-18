package co.bvc.com.test;

import java.util.Iterator;

import quickfix.DataDictionary;
import quickfix.Field;
import quickfix.FieldMap;
import quickfix.FieldNotFound;
import quickfix.FieldType;
import quickfix.Group;
import quickfix.field.MsgType;
import quickfix.fix44.Message;

public class Translate {

	public void print(DataDictionary dictionary, Message message) throws FieldNotFound {
		String msgType = message.getHeader().getString(MsgType.FIELD);
		printFieldMap("", dictionary, msgType, message.getHeader());
		printFieldMap("", dictionary, msgType, message);
		printFieldMap("", dictionary, msgType, message.getTrailer());
	}

	private void printFieldMap(String prefix, DataDictionary dictionary, String msgType, FieldMap fieldMap)
			throws FieldNotFound {

		Iterator fieldIterator = fieldMap.iterator();
		while (fieldIterator.hasNext()) {
			Field field = (Field) fieldIterator.next();
			if (!isGroupCountField(dictionary, field)) {
				String value = fieldMap.getString(field.getTag());
				if (dictionary.hasFieldValue(field.getTag())) {
					value = dictionary.getValueName(field.getTag(), fieldMap.getString(field.getTag())) + " (" + value
							+ ")";
				}
				System.out.println(prefix + dictionary.getFieldName(field.getTag()) + ": " + value);
			}
		}

		Iterator groupsKeys = fieldMap.groupKeyIterator();
		while (groupsKeys.hasNext()) {
			int groupCountTag = ((Integer) groupsKeys.next()).intValue();
			System.out.println(
					prefix + dictionary.getFieldName(groupCountTag) + ": count = " + fieldMap.getInt(groupCountTag));
			Group g = new Group(groupCountTag, 0);
			int i = 1;
			while (fieldMap.hasGroup(i, groupCountTag)) {
				if (i > 1) {
					System.out.println(prefix + "  ----");
				}
				fieldMap.getGroup(i, g);
				printFieldMap(prefix + "  ", dictionary, msgType, g);
				i++;
			}
		}
	}

	private boolean isGroupCountField(DataDictionary dictionary, Field field) {
		return dictionary.getFieldType(field.getTag()) == FieldType.NUMINGROUP;
	}

	public void print(DataDictionary dictionary, quickfix.Message message) throws FieldNotFound {
		String msgType = message.getHeader().getString(MsgType.FIELD);
		printFieldMap("", dictionary, msgType, message.getHeader());
		printFieldMap("", dictionary, msgType, message);
		printFieldMap("", dictionary, msgType, message.getTrailer());
	}
}
