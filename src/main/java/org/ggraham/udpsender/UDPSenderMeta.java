package org.ggraham.udpsender;

/*

MIT License

Copyright (c) [2017] [Gregory Graham]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/

import org.ggraham.message.PacketDecoder;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

@Step( id = "UDPSenderMeta", 
    image = "UDPSenderIcon.svg", 
    name = "UDPSenderStep.Name", 
    description = "Send UDP packets", 
    i18nPackageName = "com.hitachivantara.udpsender",
    categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Output" ) 
/**
 * 
 * Implements StepMetaInterface
 * 
 * @author ggraham
 *
 */
public class UDPSenderMeta
    extends BaseStepMeta implements StepMetaInterface {

  public static Class<?> PKG = UDPSenderMeta.class;
  
  private String m_address = "localhost";
  private String m_port = "0";
  private String m_initPoolSize = "100";
  private String m_maxPoolSize = "200";
  private String m_bigEndian = (new Boolean(true)).toString();
  private List<String> m_fieldNames = new ArrayList<String>();
  private List<PacketDecoder.PacketFieldConfig> m_fields = new ArrayList<PacketDecoder.PacketFieldConfig>();

  public String[] getFieldNames() {
	  return m_fieldNames.toArray(new String[] {});
  }
  public void setFieldNames(String[] fieldNames) {
	  m_fieldNames.clear();
	  for ( String s : fieldNames ) {
		  m_fieldNames.add(s);
	  }
  }
  public PacketDecoder.PacketFieldConfig[] getFields() {
	  return  m_fields.toArray(new PacketDecoder.PacketFieldConfig[] {}); 
  }
  public void setFields(PacketDecoder.PacketFieldConfig[] fields) {
	  m_fields.clear();
	  for (PacketDecoder.PacketFieldConfig f : fields) {
		  m_fields.add(f);
	  }
  }
  public void setFields(String[] fields) {
	  m_fields.clear();
	  for (String f : fields) {
		  m_fields.add(PacketDecoder.PacketFieldConfig.fromString(f));
	  }
  }
  
  public String getAddress() {
	  return m_address;
  }
  public void setAddress(String address) {
	  m_address = address;
  }
  
  public String getPort() {
	  return m_port;
  }
  public void setPort(String portNum) {
	  m_port = portNum;
  }

  public String getInitPoolSize() {
	  return m_initPoolSize;
  }
  public void setInitPoolSize(String initPoolSize) {
	  m_initPoolSize = initPoolSize;
  }
  public String getMaxPoolSize() {
	  return m_maxPoolSize;
  }
  public void setMaxPoolSize(String maxPoolSize) {
	  m_maxPoolSize = maxPoolSize;
  }
  public boolean getBigEndian() {
	  return Boolean.parseBoolean(m_bigEndian);
  }
  public void setBigEndian(boolean endian) {
	  m_bigEndian = (new Boolean(endian)).toString();
  }
  
  
  
  @Override public void setDefault() {
      m_port = "0";
      m_address = "localHost";
      m_initPoolSize = "100";
      m_maxPoolSize = "512";
      m_bigEndian = (new Boolean(true)).toString();
      m_fieldNames.clear();
      m_fields.clear();
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int i, TransMeta transMeta,
      Trans trans ) {
    return new UDPSenderStep( stepMeta, stepDataInterface, i, transMeta, trans );
  }

  @Override public StepDataInterface getStepData() {
    return new UDPSenderData();
  }

  @Override public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore )
      throws KettleXMLException {
	  m_address = XMLHandler.getTagValue( stepnode, "ADDRESS" );
	  m_port = XMLHandler.getTagValue( stepnode, "PORT" );
	  m_initPoolSize = XMLHandler.getTagValue( stepnode, "INITPOOLSIZE");	
	  m_maxPoolSize = XMLHandler.getTagValue( stepnode, "MAXPOOLSIZE");	
	  m_bigEndian = XMLHandler.getTagValue(stepnode,  "BIG_ENDIAN");
	  String strFieldNames = XMLHandler.getTagValue( stepnode, "FIELD_NAMES");
	  if ( strFieldNames != null && !strFieldNames.isEmpty() ) {
	  String[] fieldNames = strFieldNames.split(",");
	  setFieldNames(fieldNames);
	  String strFields = XMLHandler.getTagValue(stepnode, "FIELDS");
	  String[] fields = strFields.split(",");
	  setFields(fields);
	  }
	  
  }

  @Override public String getXML() throws KettleException {
    StringBuilder retval = new StringBuilder();
    if ( !Const.isEmpty( m_address ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "ADDRESS", m_address ) );
      }
    if ( !Const.isEmpty( m_port ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "PORT", m_port ) );
      }
    if ( !Const.isEmpty( m_initPoolSize ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "INITPOOLSIZE", m_initPoolSize ) );
      }
    if ( !Const.isEmpty( m_maxPoolSize ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "MAXPOOLSIZE", m_maxPoolSize ) );
      }
    if ( !Const.isEmpty( m_bigEndian ) ) {
        retval.append( "    " ).append( XMLHandler.addTagValue( "BIG_ENDIAN", m_bigEndian ) );
      }
    if ( m_fieldNames.size() > 0 ) {
    	String strFieldNames = String.join(",", m_fieldNames);
        retval.append( "    " ).append( XMLHandler.addTagValue( "FIELD_NAMES", strFieldNames ) );
        String[] tmp = new String[m_fields.size()];
        for (int i = 0; i < m_fields.size(); i++ ) {
        	tmp[i] = m_fields.get(i).toString();
        }
    	String strFields = String.join(",", tmp);
        retval.append( "    " ).append( XMLHandler.addTagValue( "FIELDS", strFields ) );
    }
    return retval.toString();
  }

  @Override public void readRep( Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases )
      throws KettleException {
	    m_address = rep.getStepAttributeString( stepId, "ADDRESS" );
	    m_port = rep.getStepAttributeString( stepId, "PORT" );
	    m_initPoolSize = rep.getStepAttributeString( stepId, "INITPOOLSIZE" );    
	    m_maxPoolSize = rep.getStepAttributeString( stepId, "MAXPOOLSIZE" );    
	    m_bigEndian = rep.getStepAttributeString( stepId, "BIG_ENDIAN" );    
		  String strFieldNames = rep.getStepAttributeString( stepId, "FIELD_NAMES");
		  if ( strFieldNames != null && !strFieldNames.isEmpty() ) {
		  String[] fieldNames = strFieldNames.split(",");
		  setFieldNames(fieldNames);
		  String strFields = rep.getStepAttributeString(stepId, "FIELDS");
		  String[] fields = strFields.split(",");
		  setFields(fields);
		  }
  }

  @Override public void saveRep( Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId )
      throws KettleException {
	    if ( !Const.isEmpty( m_address ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "ADDRESS", m_address );
	      }
	    if ( !Const.isEmpty( m_port ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "PORT", m_port );
	      }
	    if ( !Const.isEmpty( m_initPoolSize ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "INITPOOLSIZE", m_initPoolSize );
	      }
	    if ( !Const.isEmpty( m_maxPoolSize ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "MAXPOOLSIZE", m_maxPoolSize );
	      }
	    if ( !Const.isEmpty( m_bigEndian ) ) {
	        rep.saveStepAttribute( transformationId, stepId, "BIG_ENDIAN", m_bigEndian );
	      }
	    if ( m_fieldNames.size() > 0 ) {
	    	String strFieldNames = String.join(",", m_fieldNames);
	    	rep.saveStepAttribute( transformationId, stepId, "FIELD_NAMES", strFieldNames );
	        String[] tmp = new String[m_fields.size()];
	        for (int i = 0; i < m_fields.size(); i++ ) {
	        	tmp[i] = m_fields.get(i).toString();
	        }
	    	String strFields = String.join(",", tmp);
	    	rep.saveStepAttribute( transformationId, stepId, "FIELDS", strFields );
	    }
  }

  private static int convertTypeIds(PacketDecoder.FieldType ft) {
	  switch(ft) {
	  case DOUBLE : 
	  case FLOAT : 
		  return ValueMetaInterface.TYPE_NUMBER;
	  case INTEGER : 
	  case LONG : 
		  return ValueMetaInterface.TYPE_INTEGER;
	  case STRING : 
		  return ValueMetaInterface.TYPE_STRING;
	  case RAWBYTES : 
		  return ValueMetaInterface.TYPE_BINARY;
	  default:
		  return ValueMetaInterface.TYPE_NONE;
	  }
  }
  
  @Override
  public void getFields( RowMetaInterface rowMeta, String stepName, RowMetaInterface[] info, StepMeta nextStep,
      VariableSpace space, Repository repo, IMetaStore metaStore ) throws KettleStepException {

    rowMeta.clear();
    try {
    	for ( int i = 0; i < m_fields.size(); i++ ) {
      rowMeta.addValueMeta(ValueMetaFactory.createValueMeta( m_fieldNames.get(i), 
    		  convertTypeIds(m_fields.get(i).getFieldType()) ) );
    	}
    } catch ( KettlePluginException e ) {
      throw new KettleStepException( e );
    }
  }
}
