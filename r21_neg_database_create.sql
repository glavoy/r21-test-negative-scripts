USE [master]
GO

/****** Object:  Database [r21_neg]    Script Date: 1/20/2026 4:13:04 PM ******/
CREATE DATABASE [r21_neg]
 CONTAINMENT = NONE
 ON  PRIMARY 
( NAME = N'r21_neg', FILENAME = N'D:\R21TestNegative\SQLServerDatabase\r21_neg.mdf' , SIZE = 73728KB , MAXSIZE = UNLIMITED, FILEGROWTH = 65536KB )
 LOG ON 
( NAME = N'r21_neg_log', FILENAME = N'D:\R21TestNegative\SQLServerDatabase\r21_neg_log.ldf' , SIZE = 73728KB , MAXSIZE = 2048GB , FILEGROWTH = 65536KB )
GO

IF (1 = FULLTEXTSERVICEPROPERTY('IsFullTextInstalled'))
begin
EXEC [r21_neg].[dbo].[sp_fulltext_database] @action = 'enable'
end
GO

ALTER DATABASE [r21_neg] SET ANSI_NULL_DEFAULT OFF 
GO

ALTER DATABASE [r21_neg] SET ANSI_NULLS OFF 
GO

ALTER DATABASE [r21_neg] SET ANSI_PADDING OFF 
GO

ALTER DATABASE [r21_neg] SET ANSI_WARNINGS OFF 
GO

ALTER DATABASE [r21_neg] SET ARITHABORT OFF 
GO

ALTER DATABASE [r21_neg] SET AUTO_CLOSE OFF 
GO

ALTER DATABASE [r21_neg] SET AUTO_SHRINK OFF 
GO

ALTER DATABASE [r21_neg] SET AUTO_UPDATE_STATISTICS ON 
GO

ALTER DATABASE [r21_neg] SET CURSOR_CLOSE_ON_COMMIT OFF 
GO

ALTER DATABASE [r21_neg] SET CURSOR_DEFAULT  GLOBAL 
GO

ALTER DATABASE [r21_neg] SET CONCAT_NULL_YIELDS_NULL OFF 
GO

ALTER DATABASE [r21_neg] SET NUMERIC_ROUNDABORT OFF 
GO

ALTER DATABASE [r21_neg] SET QUOTED_IDENTIFIER OFF 
GO

ALTER DATABASE [r21_neg] SET RECURSIVE_TRIGGERS OFF 
GO

ALTER DATABASE [r21_neg] SET  DISABLE_BROKER 
GO

ALTER DATABASE [r21_neg] SET AUTO_UPDATE_STATISTICS_ASYNC OFF 
GO

ALTER DATABASE [r21_neg] SET DATE_CORRELATION_OPTIMIZATION OFF 
GO

ALTER DATABASE [r21_neg] SET TRUSTWORTHY OFF 
GO

ALTER DATABASE [r21_neg] SET ALLOW_SNAPSHOT_ISOLATION OFF 
GO

ALTER DATABASE [r21_neg] SET PARAMETERIZATION SIMPLE 
GO

ALTER DATABASE [r21_neg] SET READ_COMMITTED_SNAPSHOT OFF 
GO

ALTER DATABASE [r21_neg] SET HONOR_BROKER_PRIORITY OFF 
GO

ALTER DATABASE [r21_neg] SET RECOVERY FULL 
GO

ALTER DATABASE [r21_neg] SET  MULTI_USER 
GO

ALTER DATABASE [r21_neg] SET PAGE_VERIFY CHECKSUM  
GO

ALTER DATABASE [r21_neg] SET DB_CHAINING OFF 
GO

ALTER DATABASE [r21_neg] SET FILESTREAM( NON_TRANSACTED_ACCESS = OFF ) 
GO

ALTER DATABASE [r21_neg] SET TARGET_RECOVERY_TIME = 60 SECONDS 
GO

ALTER DATABASE [r21_neg] SET DELAYED_DURABILITY = DISABLED 
GO

ALTER DATABASE [r21_neg] SET QUERY_STORE = OFF
GO

USE [r21_neg]
GO

ALTER DATABASE SCOPED CONFIGURATION SET LEGACY_CARDINALITY_ESTIMATION = OFF;
GO

ALTER DATABASE SCOPED CONFIGURATION SET MAXDOP = 0;
GO

ALTER DATABASE SCOPED CONFIGURATION SET PARAMETER_SNIFFING = ON;
GO

ALTER DATABASE SCOPED CONFIGURATION SET QUERY_OPTIMIZER_HOTFIXES = OFF;
GO

ALTER DATABASE [r21_neg] SET  READ_WRITE 
GO




USE [r21_neg]
GO

/****** Object:  Table [dbo].[enrollee]    Script Date: 1/20/2026 4:13:51 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[enrollee](
	[starttime] [datetime2](6) NULL,
	[startdate] [date] NULL,
	[deviceid] [varchar](3) NULL,
	[deviceid2] [varchar](3) NULL,
	[mrc] [varchar](3) NULL,
	[subjid] [varchar](12) NULL,
	[district] [int] NULL,
	[mayuge_warning] [int] NULL,
	[busia_warning] [int] NULL,
	[subcounty] [varchar](2) NULL,
	[parish] [varchar](2) NULL,
	[village] [varchar](2) NULL,
	[placeofresidence] [varchar](255) NULL,
	[dob] [date] NULL,
	[age_calculated] [int] NULL,
	[agemonths_calculated] [int] NULL,
	[age_at_apr2025] [int] NULL,
	[age_in_range] [int] NULL,
	[age] [int] NULL,
	[agemonths] [int] NULL,
	[age_eligible] [int] NULL,
	[age_warning] [int] NULL,
	[age_warning_reason] [varchar](80) NULL,
	[mal_test_eligible] [int] NULL,
	[consent_eligible] [int] NULL,
	[participantsname] [varchar](80) NULL,
	[gender] [int] NULL,
	[hhheadeduclevel] [int] NULL,
	[diagnostic] [int] NULL,
	[result] [int] NULL,
	[vx_card] [int] NULL,
	[vx_any] [int] NULL,
	[vx_any_no] [int] NULL,
	[vx_any_no_oth] [varchar](80) NULL,
	[vx_doses_received] [varchar](1) NULL,
	[vx_doses_received_ver] [int] NULL,
	[vx_doses_received_ver_oth] [varchar](80) NULL,
	[vx_dose1_where] [int] NULL,
	[vx_dose1_where_oth] [varchar](80) NULL,
	[vx_dose1_date] [date] NULL,
	[vx_dose1_date_ver] [int] NULL,
	[vx_dose1_date_ver_oth] [varchar](80) NULL,
	[vx_dose2_where] [int] NULL,
	[vx_dose2_where_oth] [varchar](80) NULL,
	[vx_dose2_date] [date] NULL,
	[dose2_warning_time] [int] NULL,
	[vx_dose2_datecheck] [int] NULL,
	[vx_dose2_date_ver] [int] NULL,
	[vx_dose2_date_ver_oth] [varchar](80) NULL,
	[vx_dose3_where] [int] NULL,
	[vx_dose3_where_oth] [varchar](80) NULL,
	[vx_dose3_date] [date] NULL,
	[dose3_warning_time] [int] NULL,
	[vx_dose3_datecheck] [int] NULL,
	[vx_dose3_date_ver] [int] NULL,
	[vx_dose3_date_ver_oth] [varchar](80) NULL,
	[vx_dose4_where] [int] NULL,
	[vx_dose4_where_oth] [varchar](80) NULL,
	[vx_dose4_date] [date] NULL,
	[dose4_warning_time] [int] NULL,
	[vx_dose4_datecheck] [int] NULL,
	[vx_dose4_date_ver] [int] NULL,
	[vx_dose4_date_ver_oth] [varchar](80) NULL,
	[vx_dose_summary] [int] NULL,
	[vx_doses_miss] [int] NULL,
	[vx_doses_miss_reas] [int] NULL,
	[vx_doses_miss_reas_oth] [varchar](80) NULL,
	[vx_dose_off_sched] [int] NULL,
	[vx_doses_offsched_reas] [int] NULL,
	[vx_offsched_miss_reas_oth] [varchar](80) NULL,
	[hib_any] [int] NULL,
	[hib_doses_received] [varchar](1) NULL,
	[hib_doses_received_ver] [int] NULL,
	[hib_doses_received_ver_oth] [varchar](80) NULL,
	[hib_dose1_where] [int] NULL,
	[hib_dose1_where_oth] [varchar](80) NULL,
	[hib_dose1_date] [date] NULL,
	[hib_dose1_date_ver] [int] NULL,
	[hib_dose1_date_ver_oth] [varchar](80) NULL,
	[hib_dose2_where] [int] NULL,
	[hib_dose2_where_oth] [varchar](80) NULL,
	[hib_dose2_date] [date] NULL,
	[hibdose2_warning_time] [int] NULL,
	[hib_dose2_datecheck] [int] NULL,
	[hib_dose2_date_ver] [int] NULL,
	[hib_dose2_date_ver_oth] [varchar](80) NULL,
	[hib_dose3_where] [int] NULL,
	[hib_dose3_where_oth] [varchar](80) NULL,
	[hib_dose3_date] [date] NULL,
	[hibdose3_warning_time] [int] NULL,
	[hib_dose3_datecheck] [int] NULL,
	[hib_dose3_date_ver] [int] NULL,
	[hib_dos32_date_ver_oth] [varchar](80) NULL,
	[hib_dose_summary] [int] NULL,
	[timetobed] [int] NULL,
	[spray] [int] NULL,
	[bednetlastnight] [int] NULL,
	[bednettwoweeks] [int] NULL,
	[prevdiag] [int] NULL,
	[prevdiag_when] [int] NULL,
	[prevdiag_lastmonth] [int] NULL,
	[prevdiag_al] [int] NULL,
	[prevdiag_othermeds] [int] NULL,
	[prevdiag_descothermed] [varchar](80) NULL,
	[consent] [int] NULL,
	[consent2] [int] NULL,
	[uid] [varchar](5) NULL,
	[uid2] [varchar](5) NULL,
	[fpbarcode1_r21] [varchar](12) NULL,
	[fpbarcode2_r21] [varchar](12) NULL,
	[enroll_immrse] [int] NULL,
	[fpbarcode1_immrse] [varchar](14) NULL,
	[fpbarcode2_immrse] [varchar](14) NULL,
	[comments] [varchar](80) NULL,
	[uniqueid] [varchar](255) NULL,
	[swver] [varchar](255) NULL,
	[survey_id] [varchar](255) NULL,
	[lastmod] [datetime2](6) NULL,
	[stoptime] [datetime2](6) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[enrollee_staging]    Script Date: 1/20/2026 4:13:51 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[enrollee_staging](
	[starttime] [nvarchar](30) NULL,
	[startdate] [nvarchar](10) NULL,
	[deviceid] [nvarchar](3) NULL,
	[deviceid2] [nvarchar](3) NULL,
	[mrc] [nvarchar](3) NULL,
	[subjid] [nvarchar](12) NULL,
	[district] [nvarchar](4) NULL,
	[mayuge_warning] [nvarchar](4) NULL,
	[busia_warning] [nvarchar](4) NULL,
	[subcounty] [nvarchar](2) NULL,
	[parish] [nvarchar](2) NULL,
	[village] [nvarchar](2) NULL,
	[placeofresidence] [nvarchar](255) NULL,
	[dob] [nvarchar](10) NULL,
	[age_calculated] [nvarchar](4) NULL,
	[agemonths_calculated] [nvarchar](4) NULL,
	[age_at_apr2025] [nvarchar](4) NULL,
	[age_in_range] [nvarchar](4) NULL,
	[age] [nvarchar](4) NULL,
	[agemonths] [nvarchar](4) NULL,
	[age_eligible] [nvarchar](4) NULL,
	[age_warning] [nvarchar](4) NULL,
	[age_warning_reason] [nvarchar](80) NULL,
	[mal_test_eligible] [nvarchar](4) NULL,
	[consent_eligible] [nvarchar](4) NULL,
	[participantsname] [nvarchar](80) NULL,
	[gender] [nvarchar](4) NULL,
	[hhheadeduclevel] [nvarchar](4) NULL,
	[diagnostic] [nvarchar](4) NULL,
	[result] [nvarchar](4) NULL,
	[vx_card] [nvarchar](4) NULL,
	[vx_any] [nvarchar](4) NULL,
	[vx_any_no] [nvarchar](4) NULL,
	[vx_any_no_oth] [nvarchar](255) NULL,
	[vx_doses_received] [nvarchar](1) NULL,
	[vx_doses_received_ver] [nvarchar](4) NULL,
	[vx_doses_received_ver_oth] [nvarchar](80) NULL,
	[vx_dose1_where] [nvarchar](4) NULL,
	[vx_dose1_where_oth] [nvarchar](80) NULL,
	[vx_dose1_date] [nvarchar](10) NULL,
	[vx_dose1_date_ver] [nvarchar](4) NULL,
	[vx_dose1_date_ver_oth] [nvarchar](80) NULL,
	[vx_dose2_where] [nvarchar](4) NULL,
	[vx_dose2_where_oth] [nvarchar](80) NULL,
	[vx_dose2_date] [nvarchar](10) NULL,
	[dose2_warning_time] [nvarchar](4) NULL,
	[vx_dose2_datecheck] [nvarchar](4) NULL,
	[vx_dose2_date_ver] [nvarchar](4) NULL,
	[vx_dose2_date_ver_oth] [nvarchar](80) NULL,
	[vx_dose3_where] [nvarchar](4) NULL,
	[vx_dose3_where_oth] [nvarchar](80) NULL,
	[vx_dose3_date] [nvarchar](10) NULL,
	[dose3_warning_time] [nvarchar](4) NULL,
	[vx_dose3_datecheck] [nvarchar](4) NULL,
	[vx_dose3_date_ver] [nvarchar](4) NULL,
	[vx_dose3_date_ver_oth] [nvarchar](80) NULL,
	[vx_dose4_where] [nvarchar](4) NULL,
	[vx_dose4_where_oth] [nvarchar](80) NULL,
	[vx_dose4_date] [nvarchar](10) NULL,
	[dose4_warning_time] [nvarchar](4) NULL,
	[vx_dose4_datecheck] [nvarchar](4) NULL,
	[vx_dose4_date_ver] [nvarchar](4) NULL,
	[vx_dose4_date_ver_oth] [nvarchar](80) NULL,
	[vx_dose_summary] [nvarchar](4) NULL,
	[vx_doses_miss] [nvarchar](4) NULL,
	[vx_doses_miss_reas] [nvarchar](4) NULL,
	[vx_doses_miss_reas_oth] [nvarchar](80) NULL,
	[vx_dose_off_sched] [nvarchar](4) NULL,
	[vx_doses_offsched_reas] [nvarchar](4) NULL,
	[vx_offsched_miss_reas_oth] [nvarchar](80) NULL,
	[hib_any] [nvarchar](4) NULL,
	[hib_doses_received] [nvarchar](1) NULL,
	[hib_doses_received_ver] [nvarchar](4) NULL,
	[hib_doses_received_ver_oth] [nvarchar](80) NULL,
	[hib_dose1_where] [nvarchar](4) NULL,
	[hib_dose1_where_oth] [nvarchar](80) NULL,
	[hib_dose1_date] [nvarchar](10) NULL,
	[hib_dose1_date_ver] [nvarchar](4) NULL,
	[hib_dose1_date_ver_oth] [nvarchar](80) NULL,
	[hib_dose2_where] [nvarchar](4) NULL,
	[hib_dose2_where_oth] [nvarchar](80) NULL,
	[hib_dose2_date] [nvarchar](10) NULL,
	[hibdose2_warning_time] [nvarchar](4) NULL,
	[hib_dose2_datecheck] [nvarchar](4) NULL,
	[hib_dose2_date_ver] [nvarchar](4) NULL,
	[hib_dose2_date_ver_oth] [nvarchar](80) NULL,
	[hib_dose3_where] [nvarchar](4) NULL,
	[hib_dose3_where_oth] [nvarchar](80) NULL,
	[hib_dose3_date] [nvarchar](10) NULL,
	[hibdose3_warning_time] [nvarchar](4) NULL,
	[hib_dose3_datecheck] [nvarchar](4) NULL,
	[hib_dose3_date_ver] [nvarchar](4) NULL,
	[hib_dos32_date_ver_oth] [nvarchar](80) NULL,
	[hib_dose_summary] [nvarchar](4) NULL,
	[timetobed] [nvarchar](4) NULL,
	[spray] [nvarchar](4) NULL,
	[bednetlastnight] [nvarchar](4) NULL,
	[bednettwoweeks] [nvarchar](4) NULL,
	[prevdiag] [nvarchar](4) NULL,
	[prevdiag_when] [nvarchar](4) NULL,
	[prevdiag_lastmonth] [nvarchar](4) NULL,
	[prevdiag_al] [nvarchar](4) NULL,
	[prevdiag_othermeds] [nvarchar](4) NULL,
	[prevdiag_descothermed] [nvarchar](80) NULL,
	[consent] [nvarchar](4) NULL,
	[consent2] [nvarchar](4) NULL,
	[uid] [nvarchar](5) NULL,
	[uid2] [nvarchar](5) NULL,
	[fpbarcode1_r21] [nvarchar](12) NULL,
	[fpbarcode2_r21] [nvarchar](12) NULL,
	[enroll_immrse] [nvarchar](4) NULL,
	[fpbarcode1_immrse] [nvarchar](14) NULL,
	[fpbarcode2_immrse] [nvarchar](14) NULL,
	[comments] [nvarchar](80) NULL,
	[uniqueid] [nvarchar](255) NULL,
	[swver] [nvarchar](255) NULL,
	[survey_id] [nvarchar](255) NULL,
	[lastmod] [nvarchar](30) NULL,
	[stoptime] [nvarchar](30) NULL
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[formchanges]    Script Date: 1/20/2026 4:13:51 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[formchanges](
	[changeid] [int] IDENTITY(1,1) NOT NULL,
	[tablename] [varchar](255) NOT NULL,
	[fieldname] [varchar](255) NOT NULL,
	[uniqueid] [varchar](255) NOT NULL,
	[oldvalue] [varchar](255) NULL,
	[newvalue] [varchar](255) NULL,
	[changed_at] [datetime2](6) NULL,
PRIMARY KEY CLUSTERED 
(
	[changeid] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO

/****** Object:  Table [dbo].[formchanges_staging]    Script Date: 1/20/2026 4:13:51 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[formchanges_staging](
	[tablename] [nvarchar](255) NOT NULL,
	[fieldname] [nvarchar](255) NOT NULL,
	[uniqueid] [nvarchar](255) NOT NULL,
	[oldvalue] [nvarchar](255) NULL,
	[newvalue] [nvarchar](255) NULL,
	[changed_at] [nvarchar](255) NULL
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[formchanges] ADD  DEFAULT (getdate()) FOR [changed_at]
GO


