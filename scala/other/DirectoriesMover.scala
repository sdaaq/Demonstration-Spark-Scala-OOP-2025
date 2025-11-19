package com.example
package other

import org.apache.hadoop.fs.{FileSystem, Path}
import com.example.constants.ErrorMessage

class DirectoriesMover(fs: FileSystem) extends DirectoriesMove {
  override def move(src: String, dst: String): Unit = {
    val srcPath = new Path(src)
    val dstPath = new Path(dst)

    if (fs.exists(dstPath)) {
      fs.delete(dstPath, true)
    }

    if (!fs.rename(srcPath, dstPath)) {
      throw new RuntimeException(ErrorMessage.failRename(src, dst))
    }
  }
}
