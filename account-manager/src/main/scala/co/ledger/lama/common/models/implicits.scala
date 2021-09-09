package co.ledger.lama.common.models

import cats.implicits._
import doobie.util.meta.Meta
import io.circe.generic.extras.Configuration
import io.circe.{Json}
import io.circe.parser._
import org.postgresql.util.PGobject

object implicits {

  implicit val defaultCirceConfig: Configuration =
    Configuration.default.withSnakeCaseConstructorNames.withSnakeCaseMemberNames

  implicit val jsonMeta: Meta[Json] =
    Meta.Advanced
      .other[PGobject]("json")
      .timap[Json](a => parse(a.getValue).leftMap[Json](e => throw e).merge)(a => {
        val o = new PGobject
        o.setType("json")
        o.setValue(a.noSpaces)
        o
      })

}
