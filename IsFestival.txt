/* The source code of the Jar Archive File is to identify the property of the date variable*/
/* Date is the name of a field of the tapped transportation card record data*/
/* Return values are 'weekday', 'weekend' and 'holiday'*/

package liuxinyi;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class ISFestivals
  extends EvalFunc<String>
{
  public String exec(Tuple input)
    throws IOException
  {
    if ((input == null) || (input.size() == 0)) {
      return null;
    }
    try
    {
      String line = (String)input.get(0);
      
      String date = line.substring(0, 10);
      
      return GetMapFestival(date);
    }
    catch (Exception e)
    {
      throw new IOException(e.getMessage());
    }
  }
  
  public static void main(String[] args)
  {
    String mapFestival = GetMapFestival("2016-01-04");
    System.out.println(mapFestival);
  }
  
  public static Map<String, String> mapFestival = new HashMap();
  
  public static String GetMapFestival(String key)
  {
    if (mapFestival.containsKey(key))
    {
      if (((String)mapFestival.get(key)).matches("1")) {
        return "weekday";
      }
      if (((String)mapFestival.get(key)).matches("2")) {
        return "weekend";
      }
      return "holiday";
    }
    return "-1";
  }
  
  public static ISFestivals ih = new ISFestivals();
  
  static
  {
    try
    {
      ih.ReadFestival();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
  
  public void ReadFestival()
    throws IOException
  {
    InputStream is = getClass().getClassLoader().getResourceAsStream("festival.csv");
    Scanner scan = new Scanner(is, "UTF-8");
    
    scan.nextLine();
    while (scan.hasNext())
    {
      String[] line = scan.nextLine().split(",");
      if (line.length == 2) {
        mapFestival.put(line[0], line[1]);
      } else {
        System.out.println(line[0]);
      }
    }
    is.close();
  }
}