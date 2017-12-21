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

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.CCombo;
import org.eclipse.swt.custom.CTabFolder;
import org.eclipse.swt.custom.CTabItem;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.ShellAdapter;
import org.eclipse.swt.events.ShellEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Text;
import org.ggraham.message.PacketDecoder;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Props;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.ui.core.widget.ColumnInfo;
import org.pentaho.di.ui.core.widget.TableView;
import org.pentaho.di.ui.core.widget.TextVar;
import org.pentaho.di.ui.trans.step.BaseStepDialog;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * Implements StepDialogInterface
 * 
 * @author ggraham
 *
 */
public class UDPSenderDialog extends BaseStepDialog implements StepDialogInterface {

 protected UDPSenderMeta m_subscriberMeta;

 private CTabFolder m_wTabFolder;

 private CTabItem m_wGeneralTab;
 private TextVar m_wAddress;
 private TextVar m_wPort;
 private TextVar m_wExecuteForDuration;
 private TextVar m_wExecuteMaxPackets;

 private CTabItem m_wFieldsTab;
 private TableView m_wFieldsTable;

 private CTabItem m_wAdvancedTab;
 private TextVar m_wBufferSize;
 private TextVar m_wPoolInitSize;
 private TextVar m_wPoolMaxSize;
 
 private Button m_wBigEndian;

 
 public UDPSenderDialog( Shell parent, BaseStepMeta baseStepMeta,
                              TransMeta transMeta, String stepname ) {
   super( parent, baseStepMeta, transMeta, stepname );
   m_subscriberMeta = (UDPSenderMeta) baseStepMeta;
 }

 public UDPSenderDialog( Shell parent, Object baseStepMeta,
                              TransMeta transMeta, String stepname ) {
   super( parent, (BaseStepMeta) baseStepMeta, transMeta, stepname );
   m_subscriberMeta = (UDPSenderMeta) baseStepMeta;
 }

 public UDPSenderDialog( Shell parent, int nr, BaseStepMeta in, TransMeta tr ) {
   super( parent, nr, in, tr );
   m_subscriberMeta = (UDPSenderMeta) in;
 }

 @Override public String open() {
   Shell parent = getParent();
   Display display = parent.getDisplay();

   shell = new Shell( parent, SWT.DIALOG_TRIM | SWT.RESIZE | SWT.MIN
     | SWT.MAX );
   props.setLook( shell );
   setShellImage( shell, m_subscriberMeta );

   ModifyListener lsMod = new ModifyListener() {
     public void modifyText( ModifyEvent e ) {
       m_subscriberMeta.setChanged();
     }
   };
   changed = m_subscriberMeta.hasChanged();

   FormLayout formLayout = new FormLayout();
   formLayout.marginWidth = Const.FORM_MARGIN;
   formLayout.marginHeight = Const.FORM_MARGIN;

   shell.setLayout( formLayout );
   shell.setText( "UDP Sender" );

   int middle = props.getMiddlePct();
   int margin = Const.MARGIN;

   // Step name
   wlStepname = new Label( shell, SWT.RIGHT );
   wlStepname.setText( BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderDialog.StepName.Label" ) );
   props.setLook( wlStepname );

   fdlStepname = new FormData();
   fdlStepname.left = new FormAttachment( 0, 0 );
   fdlStepname.right = new FormAttachment( middle, -margin );
   fdlStepname.top = new FormAttachment( 0, margin );
   wlStepname.setLayoutData( fdlStepname );
   wStepname = new Text( shell, SWT.SINGLE | SWT.LEFT | SWT.BORDER );
   props.setLook( wStepname );
   wStepname.addModifyListener( lsMod );
   fdStepname = new FormData();
   fdStepname.left = new FormAttachment( middle, 0 );
   fdStepname.top = new FormAttachment( 0, margin );
   fdStepname.right = new FormAttachment( 100, 0 );
   wStepname.setLayoutData( fdStepname );
   Control lastControl = wStepname;

   // ====================
   // START OF TAB FOLDER
   // ====================
   m_wTabFolder = new CTabFolder( shell, SWT.BORDER );
   props.setLook( m_wTabFolder, Props.WIDGET_STYLE_TAB );

   // ====================
   // GENERAL TAB
   // ====================

   m_wGeneralTab = new CTabItem( m_wTabFolder, SWT.NONE );
   m_wGeneralTab.setText( BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderDialog.GeneralTab.Label" ) ); 

   FormLayout mainLayout = new FormLayout();
   mainLayout.marginWidth = 3;
   mainLayout.marginHeight = 3;

   Composite wGeneralTabComp = new Composite( m_wTabFolder, SWT.NONE );
   props.setLook( wGeneralTabComp );
   wGeneralTabComp.setLayout( mainLayout );

   // IP Address
   Label wlAddress = new Label( wGeneralTabComp, SWT.RIGHT );
   wlAddress.setText( BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderDialog.Address.Label" ) );
   props.setLook( wlAddress );
   FormData fdlAddress = new FormData();
   fdlAddress.top = new FormAttachment( 0, margin * 2 );
   fdlAddress.left = new FormAttachment( 0, 0 );
   fdlAddress.right = new FormAttachment( middle, -margin );
   wlAddress.setLayoutData( fdlAddress );
   m_wAddress = new TextVar( transMeta, wGeneralTabComp, SWT.SINGLE | SWT.LEFT
     | SWT.BORDER );
   props.setLook( m_wAddress );
   m_wAddress.addModifyListener( lsMod );
   FormData fdAddress = new FormData();
   fdAddress.top = new FormAttachment( 0, margin * 2 );
   fdAddress.left = new FormAttachment( middle, 0 );
   fdAddress.right = new FormAttachment( 100, 0 );
   m_wAddress.setLayoutData( fdAddress );
   lastControl = m_wAddress;

   // Port
   Label wlPort = new Label( wGeneralTabComp, SWT.RIGHT );
   wlPort.setText( BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderDialog.Port.Label" ) );
   props.setLook( wlPort );
   FormData fdlPort = new FormData();
   fdlPort.top = new FormAttachment( lastControl, margin  );
   fdlPort.left = new FormAttachment( 0, 0 );
   fdlPort.right = new FormAttachment( middle, -margin );
   wlPort.setLayoutData( fdlPort );
   m_wPort = new TextVar( transMeta, wGeneralTabComp, SWT.SINGLE | SWT.LEFT
     | SWT.BORDER );
   props.setLook( m_wPort );
   m_wPort.addModifyListener( lsMod );
   FormData fdPort = new FormData();
   fdPort.top = new FormAttachment( lastControl, margin );
   fdPort.left = new FormAttachment( middle, 0 );
   fdPort.right = new FormAttachment( 100, 0 );
   m_wPort.setLayoutData( fdPort );
   lastControl = m_wPort;

   FormData fdGeneralTabComp = new FormData();
   fdGeneralTabComp.left = new FormAttachment( 0, 0 );
   fdGeneralTabComp.top = new FormAttachment( 0, 0 );
   fdGeneralTabComp.right = new FormAttachment( 100, 0 );
   fdGeneralTabComp.bottom = new FormAttachment( 100, 0 );
   wGeneralTabComp.setLayoutData( fdGeneralTabComp );

   wGeneralTabComp.layout();
   m_wGeneralTab.setControl( wGeneralTabComp );
   
   // ====================
   // Fields TAB
   // ====================

   m_wFieldsTab = new CTabItem( m_wTabFolder, SWT.NONE );
   m_wFieldsTab.setText( BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderDialog.FieldsTab.Label" ) );

   FormLayout mainLayout2 = new FormLayout();
   mainLayout2.marginWidth = 3;
   mainLayout2.marginHeight = 3;

   Composite wFieldsTabComp = new Composite( m_wTabFolder, SWT.NONE );
   props.setLook( wFieldsTabComp );
   wFieldsTabComp.setLayout( mainLayout2 );
   
   PacketDecoder.FieldType[] fTypes = PacketDecoder.FieldType.values();
   String[] fTypeNames = new String[fTypes.length];
   for ( int ii = 0; ii<fTypeNames.length; ii++) fTypeNames[ii] = fTypes[ii].toString();
   
   ColumnInfo[] colinf =
		      new ColumnInfo[] {
				        new ColumnInfo( "Name", ColumnInfo.COLUMN_TYPE_TEXT ),
				        new ColumnInfo( "Type", ColumnInfo.COLUMN_TYPE_CCOMBO, fTypeNames, true ),
				        new ColumnInfo( "Encoding", ColumnInfo.COLUMN_TYPE_TEXT )
		      };

		    // colinf[ 1 ].setComboValues( ValueMetaFactory.getAllValueMetaNames() );
		    m_wFieldsTable =
		      new TableView( transMeta, wFieldsTabComp, SWT.BORDER | SWT.FULL_SELECTION | SWT.MULTI, colinf, 5, lsMod, props );
		    FormData fd = new FormData();
		    fd.left = new FormAttachment( 0, 0 );
		    fd.top = new FormAttachment( lastControl, margin );
		    fd.right = new FormAttachment( 100, 0 );
		    fd.bottom = new FormAttachment( 100, -200 );
		    m_wFieldsTable.setLayoutData( fd );

		    fd = new FormData();
		    fd.left = new FormAttachment( 0, 0 );
		    fd.top = new FormAttachment( 0, 0 );
		    fd.right = new FormAttachment( 100, 0 );
		    fd.bottom = new FormAttachment( 100, 0 );
		    wFieldsTabComp.setLayoutData( fd );
		    wFieldsTabComp.layout();
		    m_wFieldsTab.setControl( wFieldsTabComp );

   FormData fdFieldsTabComp = new FormData();
   fdFieldsTabComp.left = new FormAttachment( 0, 0 );
   fdFieldsTabComp.top = new FormAttachment( 0, 0 );
   fdFieldsTabComp.right = new FormAttachment( 100, 0 );
   fdFieldsTabComp.bottom = new FormAttachment( 100, 0 );
   wFieldsTabComp.setLayoutData( fdFieldsTabComp );

   wFieldsTabComp.layout();
   m_wFieldsTab.setControl( wFieldsTabComp );

   
   // ====================
   // Advanced TAB
   // ====================

   m_wAdvancedTab = new CTabItem( m_wTabFolder, SWT.NONE );
   m_wAdvancedTab.setText( BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderDialog.AdvancedTab.Label" ) ); 

   FormLayout mainLayout1 = new FormLayout();
   mainLayout1.marginWidth = 3;
   mainLayout1.marginHeight = 3;

   Composite wAdvancedTabComp = new Composite( m_wTabFolder, SWT.NONE );
   props.setLook( wAdvancedTabComp );
   wAdvancedTabComp.setLayout( mainLayout1 );


   // Init Pool Size
   Label wlInitPool = new Label( wAdvancedTabComp, SWT.RIGHT );
   wlInitPool.setText( BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderDialog.InitPoolSize.Label" ) );
   props.setLook( wlInitPool );
   FormData fdlInitPool = new FormData();
   fdlInitPool.top = new FormAttachment( 0, margin  );
   fdlInitPool.left = new FormAttachment( 0, 0 );
   fdlInitPool.right = new FormAttachment( middle, -margin );
   wlInitPool.setLayoutData( fdlInitPool );
   m_wPoolInitSize = new TextVar( transMeta, wAdvancedTabComp, SWT.SINGLE | SWT.LEFT
     | SWT.BORDER );
   props.setLook( m_wPoolInitSize );
   m_wPoolInitSize.addModifyListener( lsMod );
   FormData fdInitPool = new FormData();
   fdInitPool.top = new FormAttachment( 0, margin );
   fdInitPool.left = new FormAttachment( middle, 0 );
   fdInitPool.right = new FormAttachment( 100, 0 );
   m_wPoolInitSize.setLayoutData( fdInitPool );
   lastControl = m_wPoolInitSize;

   // Max Pool Size
   Label wlMaxPool = new Label( wAdvancedTabComp, SWT.RIGHT );
   wlMaxPool.setText( BaseMessages.getString( UDPSenderMeta.PKG, "UDPSenderDialog.MaxPoolSize.Label" ) );
   props.setLook( wlMaxPool );
   FormData fdlMaxPool = new FormData();
   fdlMaxPool.top = new FormAttachment( lastControl, margin  );
   fdlMaxPool.left = new FormAttachment( 0, 0 );
   fdlMaxPool.right = new FormAttachment( middle, -margin );
   wlMaxPool.setLayoutData( fdlMaxPool );
   m_wPoolMaxSize = new TextVar( transMeta, wAdvancedTabComp, SWT.SINGLE | SWT.LEFT
     | SWT.BORDER );
   props.setLook( m_wPoolMaxSize );
   m_wPoolMaxSize.addModifyListener( lsMod );
   FormData fdMaxPool = new FormData();
   fdMaxPool.top = new FormAttachment( lastControl, margin );
   fdMaxPool.left = new FormAttachment( middle, 0 );
   fdMaxPool.right = new FormAttachment( 100, 0 );
   m_wPoolMaxSize.setLayoutData( fdMaxPool );
   lastControl = m_wPoolMaxSize;

   Label wlBigEndian = new Label( wAdvancedTabComp, SWT.RIGHT );
   wlBigEndian.setText( BaseMessages
       .getString( UDPSenderMeta.PKG,
    		   "UDPSenderDialog.BigEndian.Label" ) );
   props.setLook( wlBigEndian );
   fd = new FormData();
   fd.left = new FormAttachment( 0, 0 );
   fd.top = new FormAttachment( lastControl, margin * 2 );
   fd.right = new FormAttachment( middle, -margin );
   wlBigEndian.setLayoutData( fd );

   m_wBigEndian = new Button( wAdvancedTabComp, SWT.CHECK );
   props.setLook( m_wBigEndian );
   fd = new FormData();
   fd.left = new FormAttachment( middle, 0 );
   fd.top = new FormAttachment( lastControl, margin * 2 );
   fd.right = new FormAttachment( 100, 0 );
   m_wBigEndian.setLayoutData( fd );
   lastControl = m_wBigEndian;

   FormData fdAdvancedTabComp = new FormData();
   fdAdvancedTabComp.left = new FormAttachment( 0, 0 );
   fdAdvancedTabComp.top = new FormAttachment( 0, 0 );
   fdAdvancedTabComp.right = new FormAttachment( 100, 0 );
   fdAdvancedTabComp.bottom = new FormAttachment( 100, 0 );
   wAdvancedTabComp.setLayoutData( fdAdvancedTabComp );

   wAdvancedTabComp.layout();
   m_wAdvancedTab.setControl( wAdvancedTabComp );

   // ====================
   // BUTTONS
   // ====================
   wOK = new Button( shell, SWT.PUSH );
   wOK.setText( BaseMessages.getString( UDPSenderMeta.PKG, "System.Button.OK" ) ); 
   wCancel = new Button( shell, SWT.PUSH );
   wCancel.setText( BaseMessages.getString( UDPSenderMeta.PKG, "System.Button.Cancel" ) ); 

   setButtonPositions( new Button[] { wOK, wCancel }, margin, null );

   // ====================
   // END OF TAB FOLDER
   // ====================
   FormData fdTabFolder = new FormData();
   fdTabFolder.left = new FormAttachment( 0, 0 );
   fdTabFolder.top = new FormAttachment( wStepname, margin );
   fdTabFolder.right = new FormAttachment( 100, 0 );
   fdTabFolder.bottom = new FormAttachment( wOK, -margin );
   m_wTabFolder.setLayoutData( fdTabFolder );

   // Add listeners
   lsCancel = new Listener() {
     public void handleEvent( Event e ) {
       cancel();
     }
   };
   lsOK = new Listener() {
     public void handleEvent( Event e ) {
       ok();
     }
   };
   wCancel.addListener( SWT.Selection, lsCancel );
   wOK.addListener( SWT.Selection, lsOK );

   lsDef = new SelectionAdapter() {
     public void widgetDefaultSelected( SelectionEvent e ) {
       ok();
     }
   };
   wStepname.addSelectionListener( lsDef );
   // m_wInputField.addSelectionListener( lsDef );

   m_wTabFolder.setSelection( 0 );

   // Detect X or ALT-F4 or something that kills this window...
   shell.addShellListener( new ShellAdapter() {
     public void shellClosed( ShellEvent e ) {
       cancel();
     }
   } );

   // Set the shell size, based upon previous time...
   setSize( shell, 440, 350, true );

   getData( m_subscriberMeta, true );
   m_subscriberMeta.setChanged( changed );

   shell.open();
   while ( !shell.isDisposed() ) {
     if ( !display.readAndDispatch() ) {
       display.sleep();
     }
   }
   return stepname;
 }

 private void ok() {
   if ( !Const.isEmpty( wStepname.getText() ) ) {
     setData( m_subscriberMeta );
     stepname = wStepname.getText();
     dispose();
   }
 }

 private void cancel() {
   stepname = null;
   m_subscriberMeta.setChanged( changed );
   dispose();
 }

 private void setData( UDPSenderMeta subscriberMeta ) {
	   subscriberMeta.setAddress( m_wAddress.getText() );
	   subscriberMeta.setPort( m_wPort.getText() );
	   subscriberMeta.setInitPoolSize( m_wPoolInitSize.getText() );
	   subscriberMeta.setMaxPoolSize(m_wPoolMaxSize.getText() );
	   subscriberMeta.setBigEndian(m_wBigEndian.getSelection());
	   
	    // fields
	    int nrNonEmptyFields = m_wFieldsTable.nrNonEmpty();
	    List<String> fieldNames = new ArrayList<String>();
	    List<PacketDecoder.PacketFieldConfig> fields = new ArrayList<PacketDecoder.PacketFieldConfig>();
	    for ( int i = 0; i < nrNonEmptyFields; i++ ) {
	      TableItem item = m_wFieldsTable.getNonEmpty( i );
	      fieldNames.add( item.getText( 1 ).trim() );
	      String tp = item.getText( 2 ).trim();
	      String enc = item.getText( 3 ).trim();
	      if ( enc.isEmpty() ) {
		      fields.add(new PacketDecoder.PacketFieldConfig(tp)); 	  
	      }
	      else {
		      fields.add(new PacketDecoder.PacketFieldConfig(tp, enc));
	      }
	    }
	    subscriberMeta.setFieldNames(fieldNames.toArray(new String[] {}));
	    subscriberMeta.setFields(fields.toArray(new PacketDecoder.PacketFieldConfig[] {}));
	    
	   subscriberMeta.setChanged();
 }

 private void getData( UDPSenderMeta subscriberMeta, boolean copyStepname ) {
   if ( copyStepname ) {
     wStepname.setText( stepname );
   }
   m_wAddress.setText( Const.NVL( subscriberMeta.getAddress(), "localhost" ) );
   m_wPort.setText( Const.NVL( subscriberMeta.getPort(), "0" ) );
   m_wPoolInitSize.setText( Const.NVL( subscriberMeta.getInitPoolSize(), "0" ) );
   m_wPoolMaxSize.setText( Const.NVL( subscriberMeta.getMaxPoolSize(), "0" ) );
   m_wBigEndian.setSelection(subscriberMeta.getBigEndian());

   String[] fieldNames = subscriberMeta.getFieldNames();
   PacketDecoder.PacketFieldConfig[] fields = subscriberMeta.getFields();
   if ( fields.length > 0 ) {
     m_wFieldsTable.clearAll( false );
     Table table = m_wFieldsTable.getTable();
     for ( int i = 0; i < fields.length; i++ ) {
       TableItem item = new TableItem( table, SWT.NONE );
       item.setText( 1, fieldNames[i].trim() );
       item.setText( 2, fields[i].getFieldType().toString() );
       item.setText( 3, fields[i].getEncoding().trim() );
     }

     m_wFieldsTable.removeEmptyRows();
     m_wFieldsTable.setRowNums();
     m_wFieldsTable.optWidth( true );
   }

 
 }
}
