
public class XmlHelper {
	public static String getAttributeData(String xmlEvent, String attribute){
		int begin = xmlEvent.indexOf("<"+attribute+">")+2+attribute.length();
		int end = xmlEvent.indexOf("</"+attribute+">");
		String result = xmlEvent.substring(begin, end);
		return result;
	}
}
