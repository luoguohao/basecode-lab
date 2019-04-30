import scala.Predef;

public class Test {

  public static void main(String[] args) {
    scala.collection.mutable.Map<String, String> map = new scala.collection.mutable.HashMap<String, String>();
    map.$plus$eq(new scala.Tuple2("t", "t1"));
    System.out.println(map.get("t"));
    scala.collection.immutable.Map<String, String> map2 = map.toMap(Predef.conforms());
    System.out.println(map2.get("t"));
  }
}
