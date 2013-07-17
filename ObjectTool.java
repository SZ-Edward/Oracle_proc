import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
/**
 * 
 *  @Description: 用反射机制取Map里面键对应的值赋给VO
 *	@author: aurora
 *  @date: 2012-5-30 下午04:15:08
 */
public class ObjectTool {
	
	@SuppressWarnings({ "unused", "unchecked" })
	public static void setVOAttributeValue(Map map, Object vo){
		if(map != null && vo != null){
			Method[] method = vo.getClass().getDeclaredMethods();
			for(Method m : method){
				String methodName = m.getName();
				if(methodName.contains("set")){	//set方法过滤下来，将map里面的值（键为属性）取出来，赋给vo
					String attributeName = methodName.substring(methodName.indexOf("set") + 3);	//首字母大写打头的属性名
					String attribute = attributeName.substring(0, 1).toLowerCase().concat(attributeName.substring(1, attributeName.length()));

					try {
						if(map.get(attribute) != null){
							Object attributeValue = map.get(attribute);							
							m.invoke(vo, attributeValue);
						}					
					} catch (IllegalArgumentException e) {
						e.printStackTrace();
					} catch (SecurityException e) {
						e.printStackTrace();
					} catch (IllegalAccessException e) {
						e.printStackTrace();
					} catch (InvocationTargetException e) {
						e.printStackTrace();
					}  
				}	
			}
		}
	}
	
}
