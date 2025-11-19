package com.example
package other

import org.apache.hadoop.fs.{FileSystem, Path}

class DirectoriesCleaner(fs: FileSystem) extends DirectoriesClean {
  override def cleanUp(dirs: Seq[String]): Unit = {
    dirs.foreach { dir =>
      val path = new Path(dir)
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
    }
  }

}
