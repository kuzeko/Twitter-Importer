SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

DROP SCHEMA IF EXISTS `twitter` ;
CREATE SCHEMA IF NOT EXISTS `twitter` DEFAULT CHARACTER SET utf8 ;
USE `twitter` ;

-- -----------------------------------------------------
-- Table `twitter`.`tweet`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`tweet` ;

CREATE  TABLE IF NOT EXISTS `twitter`.`tweet` (
  `id` BIGINT UNSIGNED NOT NULL ,
  `user_id` BIGINT UNSIGNED NOT NULL ,
  `in_reply_to_status_id` BIGINT UNSIGNED NOT NULL DEFAULT 0 ,
  `in_reply_to_user_id` BIGINT UNSIGNED NOT NULL DEFAULT 0 ,
  `favorited` TINYINT(1) NOT NULL DEFAULT false ,
  `retweeted` TINYINT(1) NOT NULL DEFAULT false ,
  `retweet_count` INT UNSIGNED NOT NULL DEFAULT 0 ,
  `lang` CHAR(2) NOT NULL DEFAULT '--' ,
  `created_at` DATETIME NOT NULL ,
  PRIMARY KEY (`id`, `user_id`) )
ENGINE = MyISAM;

CREATE INDEX `USER_ID` USING BTREE ON `twitter`.`tweet` (`user_id` ASC) ;

CREATE INDEX `DATE` ON `twitter`.`tweet` (`created_at` ASC) ;


-- -----------------------------------------------------
-- Table `twitter`.`tweet_text`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`tweet_text` ;

CREATE  TABLE IF NOT EXISTS `twitter`.`tweet_text` (
  `tweet_id` BIGINT UNSIGNED NOT NULL ,
  `user_id` BIGINT UNSIGNED NOT NULL ,
  `text` VARCHAR(160) NOT NULL DEFAULT '' ,
  `lat` DECIMAL(18,12) NULL ,
  `long` DECIMAL(18,12) NULL ,
  `place_full_name` VARCHAR(160) NULL ,
  `place_id` VARCHAR(160) NULL ,
  PRIMARY KEY (`tweet_id`, `user_id`) )
ENGINE = MyISAM;


-- -----------------------------------------------------
-- Table `twitter`.`user`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`user` ;

CREATE  TABLE IF NOT EXISTS `twitter`.`user` (
  `id` BIGINT UNSIGNED NOT NULL ,
  `screen_name` VARCHAR(45) NULL ,
  `name` VARCHAR(160) NULL ,
  `verified` TINYINT(1) NULL ,
  `protected` TINYINT(1) NULL ,
  `followers_count` INT UNSIGNED NOT NULL DEFAULT 0 ,
  `friends_count` INT UNSIGNED NOT NULL DEFAULT 0 ,
  `statuses_count` INT UNSIGNED NOT NULL DEFAULT 0 ,
  `favourites_count` INT UNSIGNED NOT NULL DEFAULT 0 ,
  `location` VARCHAR(160) NULL ,
  `utc_offset` INT UNSIGNED NOT NULL ,
  `time_zone` VARCHAR(45) NULL ,
  `geo_enabled` TINYINT(1) NULL ,
  `lang` CHAR(2) NULL ,
  `description` VARCHAR(160) NULL ,
  `url` VARCHAR(160) NULL ,
  `created_at` DATETIME NULL ,
  PRIMARY KEY (`id`) )
ENGINE = MyISAM;

CREATE INDEX `LANG` ON `twitter`.`user` (`lang` ASC) ;


-- -----------------------------------------------------
-- Table `twitter`.`tweet_hashtag`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`tweet_hashtag` ;

CREATE  TABLE IF NOT EXISTS `twitter`.`tweet_hashtag` (
  `tweet_id` BIGINT UNSIGNED NOT NULL ,
  `user_id` BIGINT UNSIGNED NOT NULL ,
  `hashtag_id` BIGINT UNSIGNED NOT NULL ,
  PRIMARY KEY (`tweet_id`, `hashtag_id`, `user_id`) )
ENGINE = MyISAM;


-- -----------------------------------------------------
-- Table `twitter`.`hashtag`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`hashtag` ;

CREATE  TABLE IF NOT EXISTS `twitter`.`hashtag` (
  `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT ,
  `hashtag` VARCHAR(160) NOT NULL ,
  PRIMARY KEY (`id`) )
ENGINE = MyISAM;

CREATE UNIQUE INDEX `hashtag_UNIQUE` ON `twitter`.`hashtag` (`hashtag` ASC) ;


-- -----------------------------------------------------
-- Table `twitter`.`tweet_url`
-- -----------------------------------------------------
DROP TABLE IF EXISTS `twitter`.`tweet_url` ;

CREATE  TABLE IF NOT EXISTS `twitter`.`tweet_url` (
  `tweet_id` BIGINT UNSIGNED NOT NULL ,
  `progressive` SMALLINT UNSIGNED NOT NULL ,
  `url` TEXT NULL ,
  PRIMARY KEY (`tweet_id`, `progressive`) )
ENGINE = MyISAM;



SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
