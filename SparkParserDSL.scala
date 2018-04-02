import org.apache.spark.sql.Row 
import org.apache.spark.rdd.RDD

object input extends Serializable {
  var _separator = ""
  var _rdd : Option[RDD[String]] = None
  
  
  def separator(sep : String) = _separator = sep
  def separator = _separator
  
  def set(prdd : RDD[String]) = _rdd = Some(prdd)
  
  def createRows(codeBlock: => Unit) : RDD[Row]= {
    codeBlock

    val l_sep = _separator
    val f_list = accumulator.getList
    f_list.foreach { f => f.display }

    val l_rdd = _rdd.get
    val rdd_row = l_rdd.map { f => {
        val lineSplit = f.split(l_sep)
        val seqRow = Seq.newBuilder[Any]
        f_list.foreach {_field => { 
           seqRow += _field.apply(lineSplit)
          }
        }
        Row.fromSeq(seqRow.result)
        
      }      
    }
    
    rdd_row
  }
}


object accumulator extends Serializable {
  val b = List.newBuilder[TransformationField]
  def add(f: TransformationField) {
    b.+=(f)
  }
  
  def getList() = b.result()
}

object field extends Serializable {
  def position(i : Int) : TransformationField = {
    val _position = i
    val f = new TransformationField()
    f.position(_position)
    accumulator.add(f)
    f
  }
}

class TransformationField extends Serializable {
  var _validationType : String = ""
  var _convertionType : String = ""
  var _position : Int = 0
  
  def validate(validateType: String) : TransformationField = {
    _validationType = validateType
    this
  }
  
  def convert(conversionType: String) : TransformationField = {
    _convertionType = conversionType
    this
  }
  
  def position(pos : Int) : TransformationField = {
    _position = pos
    this
  }
  
  def position = _position

  def display() = {
    System.out.println("TransformationField field# " + _position + " validation: " + _validationType + " conversion: "  + _convertionType)
  }
  
  def apply(fs : Array[String]) : Any = {
    fs(_position).toFloat
  }
}


val rdd = sc.textFile("test.csv")

input set rdd
input separator ","
val rdd2 = input createRows {
    field position 1 validate "string" convert "toFloat"
    field position 2 validate "string" convert "toFloat"
}


//rdd2.collect().foreach(println)



