package postgapspark

import java.io.File

object PostgapData {

  private[ot.postgapspark] def filePath = {
    val resource = this.getClass.getClassLoader.getResource("postgapspark/sample.dat")
    if (resource == null) sys.error("no file there where you said")
    new File(resource.toURI).getPath
  }

  private[ot.postgapspark] def parse(line: String): PGLine = {
    val subs = "</title><text>"
    val i = line.indexOf(subs)
    val title = line.substring(14, i)
    val text  = line.substring(i + subs.length, line.length-16)
    PGLine(title, text)
  }
}
