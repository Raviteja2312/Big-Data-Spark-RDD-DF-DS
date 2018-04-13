package ReadProperties
import java.io.FileInputStream
import java.util.Properties
class ReadProperties {

  var localPropertyFile = ""

  /**
    * Takes Path to the property file as a String and sets the property
    *
    * @param propertyPath Path to the property file
    */
  def setPropertyFile(propertyPath: String): Unit = {

    localPropertyFile = propertyPath

  }

  /**
    * Takes a inputKey as a parameters and return the value that is associated with the key in the property file
    *
    * @param inputKey - the key from the property file
    * @return
    * WARNING: The inputKey parameter should exactly match the key in property file.
    */

  def getProperty(inputKey: String): String = {

    /*
    * This code loads the property file and return the value based on the parameters passed.
    *
    * */
    val outputKey =
      try {
        val prop = new Properties()
        prop.load(new FileInputStream(localPropertyFile))
        prop.getProperty(inputKey)
      }
      catch {
        case e: Exception =>
          e.printStackTrace()
          sys.exit(1)
      }
    outputKey

  }


}
